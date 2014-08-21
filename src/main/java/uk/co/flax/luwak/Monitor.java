package uk.co.flax.luwak;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
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
        this.writer = new IndexWriter(directory, new IndexWriterConfig(new WhitespaceAnalyzer()));

        this.manager = new SearcherManager(writer, true, new SearcherFactory());
    }

    public Monitor(QueryCache queryCache, Presearcher presearcher) throws IOException {
        this(queryCache, presearcher, new RAMDirectory());
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
                Query matchQuery = this.queryCache.get(query.getQuery());
                if (!Strings.isNullOrEmpty(query.getHighlightQuery()))
                    this.queryCache.get(query.getHighlightQuery()); // force HlQ to be parsed
                writer.updateDocument(new Term(Monitor.FIELDS.id, query.getId()), buildIndexableQuery(query, matchQuery));
            }
            catch (Exception e) {
                errors.add(new QueryError(query.getId(), query.getQuery(), e.getMessage()));
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
            protected void doSearch(String queryId, String matchQuery, String highlight) {
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
            protected void doSearch(String id, String matchQuery, String highlight) {
                queries[0] = new MonitorQuery(id, matchQuery, highlight);
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
        doc.add(new BinaryDocValuesField(Monitor.FIELDS.query, new BytesRef(mq.getQuery())));
        String hl = mq.getHighlightQuery();
        if (hl == null)
            hl = "";
        doc.add(new BinaryDocValuesField(Monitor.FIELDS.highlight, new BytesRef(hl)));
        return doc;
    }

    // For each query selected by the presearcher, pass on to a CandidateMatcher
    private class SearchingCollector extends MonitorQueryCollector {

        final CandidateMatcher matcher;

        private SearchingCollector(CandidateMatcher matcher) {
            this.matcher = matcher;
        }

        @Override
        protected void doSearch(String queryId, String matchQuery, String highlight) {
            try {
                Query m = queryCache.get(matchQuery);
                Query h = queryCache.get(highlight);
                matcher.matchQuery(queryId, m, h);
            }
            catch (Exception e) {
                matcher.reportError(new MatchError(queryId, e));
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

        BytesRef query;
        BytesRef highlight;
        BytesRef id;

        /**
         * Do something with the matching query
         * @param id the queryId
         * @param matchQuery the matching query
         * @param highlight an optional highlighting query.  May be null.
         */
        protected abstract void doSearch(String id, String matchQuery, String highlight);

        private int queryCount = 0;
        private long searchTime = -1;

        @Override
        public void setScorer(Scorer scorer) throws IOException {

        }

        @Override
        public final void collect(int doc) throws IOException {
            query = queryDV.get(doc);
            highlight = highlightDV.get(doc);
            id = idDV.get(doc);
            queryCount++;
            doSearch(id.utf8ToString(), query.utf8ToString(), highlight.utf8ToString());
        }

        @Override
        public final void doSetNextReader(AtomicReaderContext context) throws IOException {
            this.queryDV = context.reader().getBinaryDocValues(Monitor.FIELDS.query);
            this.highlightDV = context.reader().getBinaryDocValues(Monitor.FIELDS.highlight);
            this.idDV = context.reader().getBinaryDocValues(FIELDS.id);
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
