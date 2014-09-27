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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.search.intervals.Interval;
import org.apache.lucene.search.intervals.IntervalCollector;
import org.apache.lucene.search.intervals.IntervalIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import uk.co.flax.luwak.presearcher.PresearcherMatches;
import uk.co.flax.luwak.presearcher.TermsEnumFilter;

/**
 * Copyright (c) 2014 Lemur Consulting Ltd.
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

public class Monitor implements Closeable {

    private final MonitorQueryParser queryParser;
    private final Presearcher presearcher;

    private final Directory directory;
    private final IndexWriter writer;
    private final SearcherManager manager;

    /* Used to cache updates while a purge is ongoing */
    private volatile Map<BytesRef, CacheEntry> purgeCache = null;

    /* Used to lock around the creation of the purgeCache */
    private final ReadWriteLock purgeLock = new ReentrantReadWriteLock();

    /* The current query cache */
    private Map<BytesRef, CacheEntry> queries = new ConcurrentHashMap<>();
        // NB this is not final because it can be replaced by purgeCache()

    public static final class FIELDS {
        public static final String id = "_id";
        public static final String hash = "_hash";
    }

    private final ScheduledExecutorService purgeExecutor;

    private long lastPurged = -1;

    /**
     * Create a new Monitor instance, using a passed in Directory for its queryindex
     * @param queryParser the query parser to use
     * @param presearcher the presearcher to use
     * @param directory the directory where the queryindex is stored
     * @throws IOException
     */
    public Monitor(MonitorQueryParser queryParser, Presearcher presearcher, Directory directory) throws IOException {
        this.queryParser = queryParser;
        this.presearcher = presearcher;
        this.directory = directory;

        IndexWriterConfig iwc = new IndexWriterConfig(Constants.VERSION, new WhitespaceAnalyzer(Constants.VERSION));
        this.writer = new IndexWriter(directory, configureIndexWriterConfig(iwc));

        this.manager = new SearcherManager(writer, true, new SearcherFactory());

        this.purgeExecutor = Executors.newSingleThreadScheduledExecutor();

        long purgeFrequency = configurePurgeFrequency();
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

    public Monitor(MonitorQueryParser queryParser, Presearcher presearcher) throws IOException {
        this(queryParser, presearcher, new RAMDirectory());
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

    private static class CacheEntry {

        public final MonitorQuery mq;
        public final Query matchQuery;
        public final Query highlightQuery;
        public final BytesRef hash;

        public CacheEntry(MonitorQuery mq, BytesRef hash, Query matchQuery, Query highlightQuery) {
            this.mq = mq;
            this.hash = hash;
            this.matchQuery = matchQuery;
            this.highlightQuery = highlightQuery;
        }
    }

    /**
     * @return Statistics for the internal query index and cache
     */
    public CacheStats getStats() {
        return new CacheStats(this.writer.numDocs(), this.queries.size(), lastPurged);
    }

    private void commit(List<CacheEntry> updates) throws IOException {
        purgeLock.readLock().lock();
        try {
            if (updates != null) {
                for (CacheEntry update : updates) {
                    this.queries.put(update.hash, update);
                    if (purgeCache != null)
                        purgeCache.put(update.hash, update);
                }
            }
            writer.commit();
            manager.maybeRefresh();
        }
        finally {
            purgeLock.readLock().unlock();
        }
    }

    /**
     * Remove unused queries from the query cache.
     *
     * This is normally called from a background thread at a rate set by configurePurgeFrequency().
     *
     * @throws IOException
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

        match(new MatchAllDocsQuery(), new SearchingCollector() {
            @Override
            protected void doSearch(String id, BytesRef hash) {
                newCache.put(hash.clone(), queries.get(hash));
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
     * Configure the IndexWriterConfig for the internal query cache
     * @param iwc the default IndexWriterConfig
     * @return the IndexWriterConfig to use
     */
    protected IndexWriterConfig configureIndexWriterConfig(IndexWriterConfig iwc) {
        TieredMergePolicy mergePolicy = new TieredMergePolicy();
        mergePolicy.setSegmentsPerTier(4);
        iwc.setMergePolicy(mergePolicy);
        return iwc;
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

    @Override
    public void close() throws IOException {
        purgeExecutor.shutdown();
        IOUtils.closeWhileHandlingException(manager, writer, directory);
    }

    /**
     * Add new queries to the monitor
     * @param queries the MonitorQueries to add
     * @return a list of exceptions for queries that could not be added
     * @throws IOException
     */
    public List<QueryError> update(Iterable<MonitorQuery> queries) throws IOException {

        List<QueryError> errors = new ArrayList<>();
        List<CacheEntry> updates = new ArrayList<>();

        for (MonitorQuery query : queries) {
            try {
                CacheEntry cacheEntry = createCacheEntry(query);
                updates.add(cacheEntry);
                writer.updateDocument(new Term(Monitor.FIELDS.id, query.getId()),
                        buildIndexableQuery(query.getId(), cacheEntry));
            } catch (Exception e) {
                errors.add(new QueryError(query.getId(), query.getQuery(), e.getMessage()));
            }
        }

        commit(updates);
        return errors;
    }

    private CacheEntry createCacheEntry(MonitorQuery query) throws Exception {
        Query q = queryParser.parse(query.getQuery(), query.getMetadata());
        Query hq = Strings.isNullOrEmpty(query.getHighlightQuery())
                ? null : queryParser.parse(query.getHighlightQuery(), query.getMetadata());
        return new CacheEntry(query, query.hash(), q, hq);
    }

    /**
     * Add new queries to the monitor
     * @param queries the MonitorQueries to add
     * @return a list of exceptions for queries that could not be added
     * @throws IOException
     */
    public List<QueryError> update(MonitorQuery... queries) throws IOException {
        return update(Arrays.asList(queries));
    }

    /**
     * Delete queries from the monitor
     * @param queries the queries to remove
     * @throws IOException
     */
    public void delete(Iterable<MonitorQuery> queries) throws IOException {
        for (MonitorQuery mq : queries) {
            writer.deleteDocuments(new Term(Monitor.FIELDS.id, mq.getId()));
        }
        commit(null);
    }

    /**
     * Delete queries from the monitor by ID
     * @param queryIds the IDs to delete
     * @throws IOException
     */
    public void deleteById(Iterable<String> queryIds) throws IOException {
        for (String queryId : queryIds) {
            writer.deleteDocuments(new Term(FIELDS.id, queryId));
        }
        commit(null);
    }

    /**
     * Delete queries from the monitor by ID
     * @param queryIds the IDs to delete
     * @throws IOException
     */
    public void deleteById(String... queryIds) throws IOException {
        deleteById(Arrays.asList(queryIds));
    }

    /**
     * Delete all queries from the monitor
     * @throws IOException
     */
    public void clear() throws IOException {
        writer.deleteDocuments(new MatchAllDocsQuery());
        commit(null);
    }

    private void match(CandidateMatcher matcher) throws IOException {

        long buildTime = System.nanoTime();
        Query query = buildQuery(matcher.getDocument());
        buildTime = (System.nanoTime() - buildTime) / 1000000;

        MatchingCollector collector = new MatchingCollector(matcher);
        match(query, collector);
        matcher.finish(buildTime, collector.getQueryCount());

    }

    @VisibleForTesting
    Query buildQuery(InputDocument doc) throws IOException {
        try (TermsEnumFilter filter = new TermsEnumFilter(writer)) {
            return presearcher.buildQuery(doc, filter);
        }
    }

    /**
     * Match an {@link InputDocument} against the queryindex, calling a {@link CandidateMatcher} produced by the
     * supplied {@link MatcherFactory} for each matching query.
     * @param doc the InputDocument to match
     * @param factory a {@link MatcherFactory} to use to create a {@link CandidateMatcher} for the match run
     * @param <T> the type of {@link CandidateMatcher} to return
     * @return a {@link CandidateMatcher} summarizing the match run.
     * @throws IOException
     */
    public <T extends CandidateMatcher> T match(InputDocument doc, MatcherFactory<T> factory) throws IOException {
        T matcher = factory.createMatcher(doc);
        match(matcher);
        return matcher;
    }

    /**
     * Convert the supplied document to a query using the presearcher, and run it over the query
     * index, passing each query to the supplied Collector.
     * @param doc an InputDocument to match against the index
     * @param collector the Collector to call for each match
     * @throws IOException
     */
    public void match(InputDocument doc, MonitorQueryCollector collector) throws IOException {
        match(buildQuery(doc), collector);
    }

    /**
     * Get the MonitorQuery for a given query id
     * @param queryId the id of the query to get
     * @return the MonitorQuery stored for this id, or null if not found
     * @throws IOException
     */
    public MonitorQuery getQuery(String queryId) throws IOException {
        final MonitorQuery[] queryHolder = new MonitorQuery[]{ null };
        match(new TermQuery(new Term(FIELDS.id, queryId)), new SearchingCollector() {
            @Override
            protected void doSearch(String id, BytesRef hash) {
                queryHolder[0] = queries.get(hash).mq;
            }
        });
        return queryHolder[0];
    }

    /**
     * @return the number of queries stored in this Monitor
     */
    public int getQueryCount() {
        return writer.numDocs();
    }

    public <T extends QueryMatch> PresearcherMatches<T> debug(InputDocument doc,
                                                              MatcherFactory<? extends CandidateMatcher<T>> factory) throws IOException {
        PresearcherMatchCollector collector = new PresearcherMatchCollector(factory.createMatcher(doc));
        match(doc, collector);
        return collector.getMatches();
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

    protected Document buildIndexableQuery(String id, CacheEntry query) {
        Document doc = presearcher.indexQuery(query.matchQuery);
        doc.add(new StringField(Monitor.FIELDS.id, id, Field.Store.NO));
        doc.add(new BinaryDocValuesField(Monitor.FIELDS.id, new BytesRef(id)));
        doc.add(new BinaryDocValuesField(Monitor.FIELDS.hash, query.hash));
        return doc;
    }

    // For each query selected by the presearcher, pass on to a CandidateMatcher
    private static class MatchingCollector extends SearchingCollector {

        final CandidateMatcher matcher;

        private MatchingCollector(CandidateMatcher matcher) {
            this.matcher = matcher;
        }

        @Override
        protected void doSearch(String queryId, BytesRef hash) {
            try {
                CacheEntry entry = queries.get(hash);
                matcher.matchQuery(queryId, entry.matchQuery, entry.highlightQuery);
            }
            catch (Exception e) {
                matcher.reportError(new MatchError(queryId, e));
            }
        }

    }

    public static abstract class SearchingCollector extends MonitorQueryCollector {

        /**
         * Do something with the matching query
         * @param id the queryId
         * @param hash the hash value to use to look up the query in QueryCache
         */
        protected abstract void doSearch(String id, BytesRef hash);

        @Override
        public final void collect(int doc) throws IOException {
            hashDV.get(doc, hash);
            idDV.get(doc, id);
            queryCount++;
            doSearch(id.utf8ToString(), hash);
        }

    }

    /**
     * A Collector that decodes the stored query for each document hit.
     */
    public static abstract class MonitorQueryCollector extends Collector {

        protected BinaryDocValues hashDV;
        protected BinaryDocValues idDV;
        protected AtomicReader reader;

        final BytesRef hash = new BytesRef();
        final BytesRef id = new BytesRef();

        protected Map<BytesRef, CacheEntry> queries;

        void setQueryMap(Map<BytesRef, CacheEntry> queries) {
            this.queries = queries;
        }

        protected int queryCount = 0;

        @Override
        public void setScorer(Scorer scorer) throws IOException {

        }

        @Override
        public final void setNextReader(AtomicReaderContext context) throws IOException {
            this.reader = context.reader();
            this.hashDV = context.reader().getBinaryDocValues(Monitor.FIELDS.hash);
            this.idDV = context.reader().getBinaryDocValues(FIELDS.id);
        }

        @Override
        public boolean acceptsDocsOutOfOrder() {
            return true;
        }

        public int getQueryCount() {
            return queryCount;
        }

    }

    public static class PresearcherMatchCollector<T extends QueryMatch>
            extends MonitorQueryCollector implements IntervalCollector {

        private IntervalIterator positions;
        private StoredDocument document;
        private String currentId;

        public final Map<String, StringBuilder> matchingTerms = new HashMap<>();

        private final BytesRef scratch = new BytesRef();

        final CandidateMatcher<T> matcher;

        private PresearcherMatchCollector(CandidateMatcher<T> matcher) {
            this.matcher = matcher;
        }

        public PresearcherMatches<T> getMatches() {
            return new PresearcherMatches<>(matchingTerms, matcher);
        }

        @Override
        public void collect(int doc) throws IOException {

            idDV.get(doc, scratch);
            this.currentId = scratch.utf8ToString();

            document = reader.document(doc);
            positions.scorerAdvanced(doc);
            while(positions.next() != null) {
                positions.collect(this);
            }

            hashDV.get(doc, hash);
            queryCount++;
            try {
                CacheEntry entry = queries.get(hash);
                matcher.matchQuery(currentId, entry.matchQuery, entry.highlightQuery);
            }
            catch (Exception e) {
                matcher.reportError(new MatchError(currentId, e));
            }
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            positions = scorer.intervals(true);
        }

        @Override
        public Weight.PostingFeatures postingFeatures() {
            return Weight.PostingFeatures.OFFSETS;
        }

        @Override
        public boolean acceptsDocsOutOfOrder() {
            return false;
        }

        @Override
        public void collectLeafPosition(Scorer scorer, Interval interval, int docID) {
            String terms = document.getField(interval.field).stringValue();
            if (!matchingTerms.containsKey(currentId))
                matchingTerms.put(currentId, new StringBuilder());
            matchingTerms.get(currentId)
                    .append(" ")
                    .append(interval.field)
                    .append(":")
                    .append(terms.substring(interval.offsetBegin, interval.offsetEnd));
        }

        @Override
        public void collectComposite(Scorer scorer, Interval interval, int docID) {

        }

    }

}
