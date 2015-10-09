package uk.co.flax.luwak;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.search.spans.SpanCollector;
import org.apache.lucene.search.spans.SpanExtractor;
import org.apache.lucene.search.spans.SpanRewriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IOUtils;
import uk.co.flax.luwak.presearcher.PresearcherMatches;

/*
 * Copyright (c) 2015 Lemur Consulting Ltd.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * A Monitor contains a set of MonitorQuery objects, and runs them against
 * passed-in InputDocuments.
 */
public class Monitor implements Closeable {

    protected final MonitorQueryParser queryParser;
    protected final Presearcher presearcher;
    protected final QueryDecomposer decomposer;

    private final IndexWriter writer;
    private final SearcherManager manager;

    // package-private for testing
    final Map<IndexReader, QueryTermFilter> termFilters = new HashMap<>();

    protected long slowLogLimit = 2000000;

    protected long commitBatchSize = 5000;

    /* Used to cache updates while a purge is ongoing */
    private volatile Map<BytesRef, CacheEntry> purgeCache = null;

    /* Used to lock around the creation of the purgeCache */
    private final ReadWriteLock purgeLock = new ReentrantReadWriteLock();

    /* The current query cache */
    private Map<BytesRef, CacheEntry> queries = new ConcurrentHashMap<>();
    // NB this is not final because it can be replaced by purgeCache()

    public static final class FIELDS {
        public static final String id = "_id";
        public static final String del = "_del";
        public static final String hash = "_hash";
        public static final String mq = "_mq";
    }

    private final ScheduledExecutorService purgeExecutor;

    private long lastPurged = -1;

    /**
     * Create a new Monitor instance, using a passed in IndexWriter for its queryindex
     *
     * Note that when the Monitor is closed, both the IndexWriter and its underlying
     * Directory will also be closed.
     *
     * @param queryParser the query parser to use
     * @param presearcher the presearcher to use
     * @param indexWriter an indexWriter for the query index
     * @param decomposer the QueryDecomposer to use
     * @throws IOException on IO errors
     */
    public Monitor(MonitorQueryParser queryParser, Presearcher presearcher,
                   IndexWriter indexWriter, QueryDecomposer decomposer) throws IOException {

        this.queryParser = queryParser;
        this.presearcher = presearcher;
        this.decomposer = decomposer;
        this.writer = indexWriter;

        this.manager = new SearcherManager(writer, true, new TermsHashBuilder());

        loadCache();

        long purgeFrequency = configurePurgeFrequency();
        this.purgeExecutor = Executors.newSingleThreadScheduledExecutor();
        this.purgeExecutor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    purgeCache();
                }
                catch (Exception e) {
                    // TODO: How to deal with exceptions here?
                }
            }
        }, purgeFrequency, purgeFrequency, TimeUnit.SECONDS);
    }

    /**
     * Create a new Monitor instance, using a RAMDirectory and the default QueryDecomposer
     * @param queryParser the query parser to use
     * @param presearcher the presearcher to use
     * @throws IOException on IO errors
     */
    public Monitor(MonitorQueryParser queryParser, Presearcher presearcher) throws IOException {
        this(queryParser, presearcher, defaultIndexWriter(new RAMDirectory()), new QueryDecomposer());
    }

    /**
     * Create a new Monitor instance, using the default QueryDecomposer and IndexWriter configuration
     * @param queryParser the query parser to use
     * @param presearcher the presearcher to use
     * @param directory the directory where the queryindex is stored
     * @throws IOException on IO errors
     */
    public Monitor(MonitorQueryParser queryParser, Presearcher presearcher, Directory directory) throws IOException {
        this(queryParser, presearcher, defaultIndexWriter(directory), new QueryDecomposer());
    }

    /**
     * Create a new Monitor instance, using the default QueryDecomposer
     * @param queryParser the query parser to use
     * @param presearcher the presearcher to use
     * @param indexWriter a {@link IndexWriter} for the Monitor's query index
     * @throws IOException on IO errors
     */
    public Monitor(MonitorQueryParser queryParser, Presearcher presearcher, IndexWriter indexWriter) throws IOException {
        this(queryParser, presearcher, indexWriter, new QueryDecomposer());
    }

    private static IndexWriter defaultIndexWriter(Directory directory) throws IOException {

        IndexWriterConfig iwc = new IndexWriterConfig(new KeywordAnalyzer());
        TieredMergePolicy mergePolicy = new TieredMergePolicy();
        mergePolicy.setSegmentsPerTier(4);
        iwc.setMergePolicy(mergePolicy);
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);

        return new IndexWriter(directory, iwc);

    }

    private class TermsHashBuilder extends SearcherFactory {
        @Override
        public IndexSearcher newSearcher(IndexReader reader, IndexReader previousReader) throws IOException {
            IndexSearcher searcher = super.newSearcher(reader, previousReader);
            termFilters.put(reader, new QueryTermFilter(reader));
            reader.addReaderClosedListener(new IndexReader.ReaderClosedListener() {
                @Override
                public void onClose(IndexReader reader) throws IOException {
                    termFilters.remove(reader);
                }
            });
            return searcher;
        }
    }

    /**
     * Statistics for the query cache and query index
     */
    public static class CacheStats {

        /** Total number of queries in the query index */
        public final int queries;

        /** Total number of queries int the query cache */
        public final int cachedQueries;

        /** Time the query cache was last purged */
        public final long lastPurged;

        public CacheStats(int queries, int cachedQueries, long lastPurged) {
            this.queries = queries;
            this.cachedQueries = cachedQueries;
            this.lastPurged = lastPurged;
        }
    }

    protected static class CacheEntry {

        public final Query matchQuery;
        public final BytesRef hash;
        public final Map<String,String> metadata;

        public CacheEntry(BytesRef hash, Query matchQuery, Map<String, String> metadata) {
            this.hash = hash;
            this.matchQuery = matchQuery;
            this.metadata = metadata;
        }
    }

    protected static class Indexable {
        final String id;
        final CacheEntry cacheEntry;
        final Document document;

        private Indexable(String id, CacheEntry cacheEntry, Document document) {
            this.id = id;
            this.cacheEntry = cacheEntry;
            this.document = document;
        }
    }

    private void loadCache() throws IOException {
        final List<Exception> parseErrors = new LinkedList<>();

        match(new MatchAllDocsQuery(), new MonitorQueryCollector() {
            @Override
            public void doMatch(int doc, String queryId, BytesRef hash) {
                BytesRef serializedMQ = mqDV.get(doc);
                MonitorQuery mq = MonitorQuery.deserialize(serializedMQ);
                try {
                    for (CacheEntry ce : decomposeQuery(mq)) {
                        queries.put(ce.hash, ce);
                    }
                } catch (Exception e) {
                    parseErrors.add(e);
                }
            }
        });

        if (parseErrors.size() != 0)
            throw new IOException("Error populating cache - some queries couldn't be parsed:" + parseErrors);}

    /**
     * @return Statistics for the internal query index and cache
     */
    public CacheStats getStats() {
        return new CacheStats(this.writer.numDocs(), this.queries.size(), lastPurged);
    }

    private synchronized void commit(List<Indexable> updates) throws IOException {
        beforeCommit(updates);
        purgeLock.readLock().lock();
        try {
            if (updates != null) {
                for (Indexable update : updates) {
                    this.queries.put(update.cacheEntry.hash, update.cacheEntry);
                    writer.addDocument(update.document);
                    if (purgeCache != null)
                        purgeCache.put(update.cacheEntry.hash, update.cacheEntry);
                }
            }
            writer.commit();
            manager.maybeRefresh();
        }
        finally {
            purgeLock.readLock().unlock();
        }
        afterCommit(updates);
    }

    /**
     * Called before a commit
     * @param updates the list of updates that will be committed (null for deletes)
     */
    protected void beforeCommit(List<Indexable> updates) {}

    /**
     * Called after a commit
     * @param updates the list of updates that have been committed (null for deletes)
     */
    protected void afterCommit(List<Indexable> updates) {}

    /**
     * Remove unused queries from the query cache.
     *
     * This is normally called from a background thread at a rate set by configurePurgeFrequency().
     *
     * @throws IOException on IO errors
     */
    public synchronized void purgeCache() throws IOException {

        /*
            Note on implementation

            The purge works by scanning the query index and creating a new query cache populated
            for each query in the index.  When the scan is complete, the old query cache is swapped
            for the new, allowing it to be garbage-collected.

            In order to not drop cached queries that have been added while a purge is ongoing,
            we use a ReadWriteLock to guard the creation and removal of an update log.  Commits take
            the read lock.  If the update log has been created, then a purge is ongoing, and queries
            are added to the update log within the read lock guard.

            The purge takes the write lock when creating the update log, and then when swapping out
            the old query cache.  Within the second write lock guard, the contents of the update log
            are added to the new query cache, and the update log itself is removed.
         */

        final Map<BytesRef, CacheEntry> newCache = new ConcurrentHashMap<>();

        purgeLock.writeLock().lock();
        try {
            purgeCache = new ConcurrentHashMap<>();
        }
        finally {
            purgeLock.writeLock().unlock();
        }

        match(new MatchAllDocsQuery(), new MonitorQueryCollector() {
            @Override
            protected void doMatch(int doc, String id, BytesRef hash) {
                newCache.put(BytesRef.deepCopyOf(hash), queries.get(hash));
            }
        });

        purgeLock.writeLock().lock();
        try {
            newCache.putAll(purgeCache);
            purgeCache = null;
            Monitor.this.queries = newCache;
            lastPurged = System.nanoTime();
        }
        finally {
            purgeLock.writeLock().unlock();
        }
    }

    /**
     * Configure the frequency with which the query cache will be purged.
     *
     * Default = 5 minutes
     *
     * @return the frequency (in seconds)
     */
    protected long configurePurgeFrequency() {
        return 300;
    }

    /**
     * Set the slow log limit
     *
     * All queries that take longer than t nanoseconds to run will be recorded in
     * the slow log.  The default is 2,000,000 (2 milliseconds)
     *
     * @param limit the limit in nanoseconds
     *
     * @see Matches#getSlowLog()
     */
    public void setSlowLogLimit(long limit) {
        this.slowLogLimit = limit;
    }

    @Override
    public void close() throws IOException {
        purgeExecutor.shutdown();
        IOUtils.closeWhileHandlingException(manager, writer, writer.getDirectory());
    }

    /**
     * Add new queries to the monitor
     * @param queries the MonitorQueries to add
     * @return a list of exceptions for queries that could not be added
     * @throws IOException on IO errors
     */
    public List<QueryError> update(Iterable<MonitorQuery> queries) throws IOException {

        List<QueryError> errors = new ArrayList<>();
        List<Indexable> updates = new ArrayList<>();

        for (MonitorQuery query : queries) {
            try {
                writer.deleteDocuments(new Term(FIELDS.del, query.getId()));
                for (CacheEntry cacheEntry : decomposeQuery(query)) {
                    updates.add(new Indexable(query.getId(), cacheEntry, buildIndexableQuery(query.getId(), query, cacheEntry)));
                }
            } catch (Exception e) {
                errors.add(new QueryError(query.getId(), query.getQuery(), e.getMessage()));
            }
            if (updates.size() > commitBatchSize) {
                commit(updates);
                updates.clear();
            }
        }

        commit(updates);
        return errors;
    }

    private Iterable<CacheEntry> decomposeQuery(MonitorQuery query) throws Exception {

        Query q = queryParser.parse(query.getQuery(), query.getMetadata());

        BytesRef rootHash = query.hash();

        int upto = 0;
        List<CacheEntry> cacheEntries = new LinkedList<>();
        for (Query subquery : decomposer.decompose(q)) {
            BytesRefBuilder subHash = new BytesRefBuilder();
            subHash.append(rootHash);
            subHash.append(new BytesRef("_" + upto++));
            cacheEntries.add(new CacheEntry(subHash.toBytesRef(), subquery, query.getMetadata()));
        }

        return cacheEntries;
    }

    /**
     * Add new queries to the monitor
     * @param queries the MonitorQueries to add
     * @return a list of exceptions for queries that could not be added
     * @throws IOException on IO errors
     */
    public List<QueryError> update(MonitorQuery... queries) throws IOException {
        return update(Arrays.asList(queries));
    }

    /**
     * Delete queries from the monitor
     * @param queries the queries to remove
     * @throws IOException on IO errors
     */
    public void delete(Iterable<MonitorQuery> queries) throws IOException {
        for (MonitorQuery mq : queries) {
            writer.deleteDocuments(new Term(Monitor.FIELDS.del, mq.getId()));
        }
        commit(null);
    }

    /**
     * Delete queries from the monitor by ID
     * @param queryIds the IDs to delete
     * @throws IOException on IO errors
     */
    public void deleteById(Iterable<String> queryIds) throws IOException {
        for (String queryId : queryIds) {
            writer.deleteDocuments(new Term(FIELDS.del, queryId));
        }
        commit(null);
    }

    /**
     * Delete queries from the monitor by ID
     * @param queryIds the IDs to delete
     * @throws IOException on IO errors
     */
    public void deleteById(String... queryIds) throws IOException {
        deleteById(Arrays.asList(queryIds));
    }

    /**
     * Delete all queries from the monitor
     * @throws IOException on IO errors
     */
    public void clear() throws IOException {
        writer.deleteDocuments(new MatchAllDocsQuery());
        commit(null);
    }

    /**
     * Match an {@link InputDocument} against the queryindex, calling a {@link CandidateMatcher} produced by the
     * supplied {@link MatcherFactory} for each matching query.
     * @param doc the InputDocument to match
     * @param factory a {@link MatcherFactory} to use to create a {@link CandidateMatcher} for the match run
     * @param <T> the type of {@link CandidateMatcher} to return
     * @return a {@link CandidateMatcher} summarizing the match run.
     * @throws IOException on IO errors
     */
    public <T extends QueryMatch> Matches<T> match(InputDocument doc, MatcherFactory<T> factory) throws IOException {
        CandidateMatcher<T> matcher = factory.createMatcher(doc);
        matcher.setSlowLogLimit(slowLogLimit);
        match(matcher);
        return matcher.getMatches();
    }

    private <T extends QueryMatch> void match(CandidateMatcher<T> matcher) throws IOException {

        long buildTime = System.nanoTime();
        MatchingCollector<T> collector = new MatchingCollector<>(matcher);
        IndexSearcher searcher = null;
        try {
            searcher = manager.acquire();
            Query query = presearcher.buildQuery(matcher.getDocument(), termFilters.get(searcher.getIndexReader()));
            buildTime = (System.nanoTime() - buildTime) / 1000000;
            collector.setQueryMap(this.queries);
            searcher.search(query, collector);
        }
        finally {
            manager.release(searcher);
        }
        matcher.finish(buildTime, collector.getQueryCount());

    }

    private void match(Query query, MonitorQueryCollector collector) throws IOException {
        IndexSearcher searcher = null;
        try {
            searcher = manager.acquire();
            collector.setQueryMap(this.queries);
            searcher.search(query, collector);
        }
        finally {
            manager.release(searcher);
        }
    }

    /**
     * Get the MonitorQuery for a given query id
     * @param queryId the id of the query to get
     * @return the MonitorQuery stored for this id, or null if not found
     * @throws IOException on IO errors
     */
    public MonitorQuery getQuery(String queryId) throws IOException {
        final MonitorQuery[] queryHolder = new MonitorQuery[]{ null };
        match(new TermQuery(new Term(FIELDS.id, queryId)), new MonitorQueryCollector() {
            @Override
            public void doMatch(int doc, String queryId, BytesRef hash) {
                BytesRef serializedMQ = mqDV.get(doc);
                queryHolder[0] = MonitorQuery.deserialize(serializedMQ);
            }
        });
        return queryHolder[0];
    }

    /**
     * @return the number of queries (after decomposition) stored in this Monitor
     */
    public int getDisjunctCount() {
        return writer.numDocs();
    }

    /**
     * @return the number of queries stored in this Monitor
     * @throws java.io.IOException on IO errors
     */
    public int getQueryCount() throws IOException {
        return getQueryIds().size();
    }

    /**
     * @return the set of query ids of the queries stored in this Monitor
     */
    public Set<String> getQueryIds() throws IOException {
        final Set<String> ids = new HashSet<>();
        match(new MatchAllDocsQuery(), new MonitorQueryCollector() {
            @Override
            public void doMatch(int doc, String queryId, BytesRef hash) {
                ids.add(queryId);
            }
        });
        return ids;
    }

    /**
     * Match an InputDocument against the queries stored in the Monitor, also returning information
     * about which queries were selected by the presearcher, and why.
     * @param doc an InputDocument to match against the index
     * @param factory a {@link MatcherFactory} to use to create a {@link CandidateMatcher} for the match run
     * @param <T> the type of QueryMatch produced by the CandidateMatcher
     * @return a PresearcherMatches object
     * @throws IOException on IO errors
     */
    public <T extends QueryMatch> PresearcherMatches<T> debug(InputDocument doc, MatcherFactory<T> factory)
            throws IOException {
        IndexSearcher searcher = null;
        try {
            searcher = manager.acquire();
            PresearcherMatchCollector<T> collector = new PresearcherMatchCollector<>(factory.createMatcher(doc));
            collector.setQueryMap(queries);
            Query presearcherQuery = new ForceNoBulkScoringQuery(
                    SpanRewriter.INSTANCE.rewrite(presearcher.buildQuery(doc, termFilters.get(searcher.getIndexReader())))
            );
            searcher.search(presearcherQuery, collector);
            return collector.getMatches();
        }
        finally {
            manager.release(searcher);
        }
    }

    protected Document buildIndexableQuery(String id, MonitorQuery mq, CacheEntry query) {
        Document doc = presearcher.indexQuery(query.matchQuery, mq.getMetadata());
        doc.add(new StringField(FIELDS.id, id, Field.Store.NO));
        doc.add(new StringField(FIELDS.del, id, Field.Store.NO));
        doc.add(new SortedDocValuesField(FIELDS.id, new BytesRef(id)));
        doc.add(new BinaryDocValuesField(FIELDS.hash, query.hash));
        doc.add(new BinaryDocValuesField(FIELDS.mq, MonitorQuery.serialize(mq)));
        return doc;
    }

    // For each query selected by the presearcher, pass on to a CandidateMatcher
    private static class MatchingCollector<T extends QueryMatch> extends MonitorQueryCollector {

        final CandidateMatcher<T> matcher;

        private MatchingCollector(CandidateMatcher<T> matcher) {
            this.matcher = matcher;
        }

        @Override
        protected void doMatch(int doc, String queryId, BytesRef hash) throws IOException {
            try {
                CacheEntry entry = queries.get(hash);
                matcher.matchQuery(queryId, entry.matchQuery, entry.metadata);
            }
            catch (Exception e) {
                matcher.reportError(new MatchError(queryId, e));
            }
        }

    }

    /**
     * A Collector that decodes the stored query for each document hit.
     */
    public static abstract class MonitorQueryCollector extends SimpleCollector {

        protected BinaryDocValues hashDV;
        protected SortedDocValues idDV;
        protected BinaryDocValues mqDV;
        protected LeafReader reader;

        protected Map<BytesRef, CacheEntry> queries;

        void setQueryMap(Map<BytesRef, CacheEntry> queries) {
            this.queries = queries;
        }

        protected int queryCount = 0;

        @Override
        public void collect(int doc) throws IOException {
            BytesRef hash = hashDV.get(doc);
            BytesRef id = idDV.get(doc);
            queryCount++;
            doMatch(doc, id.utf8ToString(), hash);
        }

        protected abstract void doMatch(int doc, String queryId, BytesRef queryHash) throws IOException;

        @Override
        public void doSetNextReader(LeafReaderContext context) throws IOException {
            this.reader = context.reader();
            this.hashDV = context.reader().getBinaryDocValues(Monitor.FIELDS.hash);
            this.idDV = context.reader().getSortedDocValues(FIELDS.id);
            this.mqDV = context.reader().getBinaryDocValues(FIELDS.mq);
        }

        @Override
        public boolean needsScores() {
            return false;
        }

        public int getQueryCount() {
            return queryCount;
        }

    }

    private class PresearcherMatchCollector<T extends QueryMatch> extends MatchingCollector<T> {

        private String currentId;
        private Scorer scorer;

        public final Map<String, StringBuilder> matchingTerms = new HashMap<>();

        private PresearcherMatchCollector(CandidateMatcher<T> matcher) {
            super(matcher);
        }

        public PresearcherMatches<T> getMatches() {
            return new PresearcherMatches<>(matchingTerms, matcher.getMatches());
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            this.scorer = scorer;
        }

        @Override
        protected void doMatch(int doc, String queryId, BytesRef hash) throws IOException {

            currentId = queryId;

            SpanCollector collector = new SpanCollector() {
                @Override
                public void collectLeaf(PostingsEnum postingsEnum, int position, Term term) throws IOException {
                    if (!matchingTerms.containsKey(currentId))
                        matchingTerms.put(currentId, new StringBuilder());
                    matchingTerms.get(currentId)
                            .append(" ")
                            .append(term.field())
                            .append(":")
                            .append(term.bytes().utf8ToString());
                }

                @Override
                public void reset() {

                }
            };

            SpanExtractor.collect(scorer, collector, false);

            super.doMatch(doc, queryId, hash);
        }

    }

}
