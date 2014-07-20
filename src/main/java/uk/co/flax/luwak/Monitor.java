package uk.co.flax.luwak;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
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

    //private final MonitorQueryParser parser;
    private final QueryCache queryCache;
    private final Presearcher presearcher;

    private final Directory directory;
    private final IndexWriter writer;
    private final SearcherManager manager;

    public static final class FIELDS {
        public static final String id = "_id";
        public static final String seqId = "_seqId";
        public static final String query = "_query";
        public static final String highlight = "_highlight";
    }

    /**
     * Create a new Monitor instance, using a passed in Directory for its queryindex
     * @param queryCache the querycache to use
     * @param presearcher the presearcher to use
     * @param directory the directory where the queryindex is stored
     * @throws IOException
     */
    public Monitor(QueryCache queryCache, Presearcher presearcher, Directory directory) throws IOException {
        this.queryCache = queryCache;
        this.presearcher = presearcher;
        this.directory = directory;

        IndexWriterConfig iwc = new IndexWriterConfig(Constants.VERSION, new WhitespaceAnalyzer(Constants.VERSION));
        this.writer = new IndexWriter(directory, configureIndexWriterConfig(iwc));

        this.manager = new SearcherManager(writer, true, new SearcherFactory());
    }

    public Monitor(QueryCache queryCache, Presearcher presearcher) throws IOException {
        this(queryCache, presearcher, new RAMDirectory());
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

    @Override
    public void close() throws IOException {
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
        for (MonitorQuery query : queries) {
            try {
                QueryCacheEntry matchQuery = this.queryCache.get(query);
                writer.updateDocument(new Term(Monitor.FIELDS.id, query.getId()),
                        buildIndexableQuery(query, matchQuery.getQuery()));
            }
            catch (Exception e) {
                errors.add(new QueryError(query.getId(), query.getQuery().utf8ToString(), e.getMessage()));
            }
        }
        writer.commit();
        manager.maybeRefresh();
        return errors;
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
        writer.commit();
        manager.maybeRefresh();
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
        writer.commit();
        manager.maybeRefresh();
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
        writer.commit();
        manager.maybeRefresh();
    }

    private void match(CandidateMatcher matcher) throws IOException {

        long start = System.nanoTime();
        Query query = buildQuery(matcher.getDocument());
        matcher.setQueryBuildTime((System.nanoTime() - start) / 1000000);

        SearchingCollector collector = new SearchingCollector(matcher);
        match(query, collector);
        matcher.setQueriesRun(collector.getQueryCount());

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
    public void match(InputDocument doc, Collector collector) throws IOException {
        match(buildQuery(doc), collector);
    }

    /**
     * Ensures that all queries in the queryindex have been parsed.  Call this if you
     * have stored queries in an external Directory and want to to ensure that they are
     * all loaded and parsed before any documents are passed in.
     *
     * @throws IOException
     */
    public void loadAllQueries() throws IOException {
        match(new MatchAllDocsQuery(), new MonitorQueryCollector() {
            @Override
            protected void doSearch(MonitorQuery mq) {
                // no impl
            }
        });
    }

    /**
     * Get the MonitorQuery for a given query id
     * @param queryId the id of the query to get
     * @return the MonitorQuery stored for this id, or null if not found
     * @throws IOException
     */
    public MonitorQuery getQuery(String queryId) throws IOException {
        final MonitorQuery[] queries = new MonitorQuery[]{ null };
        match(new TermQuery(new Term(FIELDS.id, queryId)), new MonitorQueryCollector() {
            @Override
            protected void doSearch(MonitorQuery mq) {
                queries[0] = mq;
            }
        });
        return queries[0];
    }

    /**
     * @return the number of queries stored in this Monitor
     */
    public int getQueryCount() {
        return writer.numDocs();
    }

    private void match(Query query, Collector collector) throws IOException {
        IndexSearcher searcher = null;
        long startTime = System.nanoTime();
        try {
            searcher = manager.acquire();
            searcher.search(query, collector);
        }
        finally {
            manager.release(searcher);
            if (collector instanceof TimedCollector) {
                long searchTime = TimeUnit.MILLISECONDS.convert((System.nanoTime() - startTime), TimeUnit.NANOSECONDS);
                ((TimedCollector) collector).setSearchTime(searchTime);
            }
        }
    }

    protected Document buildIndexableQuery(MonitorQuery mq, Query matchQuery) {
        Document doc = presearcher.indexQuery(matchQuery);
        doc.add(new StringField(Monitor.FIELDS.id, mq.getId(), Field.Store.NO));
        doc.add(new BinaryDocValuesField(Monitor.FIELDS.id, new BytesRef(mq.getId())));
        doc.add(new BinaryDocValuesField(Monitor.FIELDS.query, mq.getQuery()));
        doc.add(new NumericDocValuesField(Monitor.FIELDS.seqId, mq.getSeqId()));
        BytesRef hl = mq.getHighlightQuery();
        if (hl == null)
            hl = new BytesRef("");
        doc.add(new BinaryDocValuesField(Monitor.FIELDS.highlight, hl));
        return doc;
    }

    // For each query selected by the presearcher, pass on to a CandidateMatcher
    private class SearchingCollector extends MonitorQueryCollector {

        final CandidateMatcher matcher;

        private SearchingCollector(CandidateMatcher matcher) {
            this.matcher = matcher;
        }

        @Override
        protected void doSearch(MonitorQuery mq) {
            try {
                QueryCacheEntry query = queryCache.get(mq);
                matcher.matchQuery(mq.getId(), query.getQuery(), query.getHighlightQuery());
            }
            catch (Exception e) {
                matcher.reportError(new MatchError(mq.getId(), e));
            }
        }

        @Override
        public void setSearchTime(long searchTime) {
            matcher.setSearchTime(searchTime);
        }
    }

    /**
     * A Collector that decodes the stored query for each document hit.
     */
    public abstract class MonitorQueryCollector extends TimedCollector {

        protected BinaryDocValues queryDV;
        protected BinaryDocValues highlightDV;
        protected BinaryDocValues idDV;
        protected NumericDocValues seqIdDV;

        final BytesRef query = new BytesRef();
        final BytesRef highlight = new BytesRef();
        final BytesRef id = new BytesRef();
        long seqId = 0;

        /**
         * Do something with the matching query
         * @param query MonitorQuery instance
         */
        protected abstract void doSearch(MonitorQuery query);

        private int queryCount = 0;
        private long searchTime = -1;

        @Override
        public void setScorer(Scorer scorer) throws IOException {

        }

        @Override
        public final void collect(int doc) throws IOException {
            queryDV.get(doc, query);
            highlightDV.get(doc, highlight);
            idDV.get(doc, id);
            seqId = seqIdDV.get(doc);
            queryCount++;
            MonitorQuery q = new MonitorQuery(id.utf8ToString(), query, highlight, seqId);
            doSearch(q);
        }

        @Override
        public final void setNextReader(AtomicReaderContext context) throws IOException {
            this.queryDV = context.reader().getBinaryDocValues(Monitor.FIELDS.query);
            this.highlightDV = context.reader().getBinaryDocValues(Monitor.FIELDS.highlight);
            this.idDV = context.reader().getBinaryDocValues(FIELDS.id);
            this.seqIdDV = context.reader().getNumericDocValues(FIELDS.seqId);
        }

        @Override
        public final boolean acceptsDocsOutOfOrder() {
            return true;
        }

        public int getQueryCount() {
            return queryCount;
        }

        public long getSearchTime() {
            return searchTime;
        }

        @Override
        public void setSearchTime(long searchTime) {
            this.searchTime = searchTime;
        }
    }

}
