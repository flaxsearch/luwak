package uk.co.flax.luwak;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.Version;
import uk.co.flax.luwak.presearcher.TermsEnumFilter;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

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

    private final MonitorQueryParser parser;
    private final Presearcher presearcher;

    private final Directory directory;
    private final IndexWriter writer;
    private final SearcherManager manager;

    // Query cache, using a passed-in QueryParser to build queries on demand.
    private final LoadingCache<String, Query> queries = CacheBuilder.newBuilder().build(new CacheLoader<String, Query>() {
        @Override
        public Query load(String query) throws Exception {
            return parser.parse(query);
        }
    });

    public static final class FIELDS {
        public static final String id = "_id";
        public static final String query = "_query";
        public static final String highlight = "_highlight";
    }

    /**
     * Create a new Monitor instance, using an internal RAMDirectory to build the queryindex
     * @param parser the query parser to use
     * @param presearcher the presearcher to use
     * @throws IOException
     */
    public Monitor(MonitorQueryParser parser, Presearcher presearcher) throws IOException {
        this(parser, presearcher, new RAMDirectory());
    }

    /**
     * Create a new Monitor instance, using a passed in Directory for its queryindex
     * @param parser the query parser to use
     * @param presearcher the presearcher to use
     * @param directory the directory where the queryindex is stored
     * @throws IOException
     */
    public Monitor(MonitorQueryParser parser, Presearcher presearcher, Directory directory) throws IOException {
        this.parser = parser;
        this.presearcher = presearcher;
        this.directory = directory;
        this.writer = new IndexWriter(directory, new IndexWriterConfig(Version.LUCENE_50,
                new WhitespaceAnalyzer(Version.LUCENE_50)));

        this.manager = new SearcherManager(writer, true, new SearcherFactory());
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
                Query matchQuery = this.queries.get(query.getQuery());
                if (!Strings.isNullOrEmpty(query.getHighlightQuery()))
                    this.queries.get(query.getHighlightQuery()); // force HlQ to be parsed
                writer.updateDocument(new Term(Monitor.FIELDS.id, query.getId()), buildIndexableQuery(query, matchQuery));
            }
            catch (ExecutionException e) {
                Throwable t = e.getCause();
                if (t instanceof MonitorQueryParserException)
                    errors.add(new QueryError(query.getId(), (MonitorQueryParserException) t));
                else
                    throw new RuntimeException(t);
            }
        }
        writer.commit();
        manager.maybeRefresh();
        return errors;
    }

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

    public <T extends CandidateMatcher> T match(InputDocument doc, MatcherFactory<T> factory) throws IOException {
        T matcher = factory.createMatcher(doc);
        match(matcher);
        return matcher;
    }

    public void match(InputDocument doc, TimedCollector collector) throws IOException {
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
            protected void doSearch(String queryId, Query matchQuery, Query highlight) {
                // no impl
            }
        });
    }

    public int getQueryCount() {
        return writer.numDocs();
    }

    private void match(Query query, TimedCollector collector) throws IOException {
        IndexSearcher searcher = null;
        long startTime = System.nanoTime();
        try {
            searcher = manager.acquire();
            searcher.search(query, collector);
        }
        finally {
            manager.release(searcher);
            collector.setSearchTime((System.nanoTime() - startTime) / 1000000);
        }
    }

    protected Document buildIndexableQuery(MonitorQuery mq, Query matchQuery) {
        Document doc = presearcher.indexQuery(matchQuery);
        doc.add(new StringField(Monitor.FIELDS.id, mq.getId(), Field.Store.NO));
        doc.add(new SortedDocValuesField(Monitor.FIELDS.id, new BytesRef(mq.getId())));
        doc.add(new SortedDocValuesField(Monitor.FIELDS.query, new BytesRef(mq.getQuery())));
        String hl = mq.getHighlightQuery();
        if (hl == null)
            hl = "";
        doc.add(new SortedDocValuesField(Monitor.FIELDS.highlight, new BytesRef(hl)));
        return doc;
    }

    private class SearchingCollector extends MonitorQueryCollector {

        final CandidateMatcher matcher;

        private SearchingCollector(CandidateMatcher matcher) {
            this.matcher = matcher;
        }

        @Override
        protected void doSearch(String queryId, Query matchQuery, Query highlight) {
            try {
                matcher.matchQuery(queryId, matchQuery, highlight);
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

    public abstract class MonitorQueryCollector extends TimedCollector {

        protected SortedDocValues queryDV;
        protected SortedDocValues highlightDV;
        protected SortedDocValues idDV;

        final BytesRef query = new BytesRef();
        final BytesRef highlight = new BytesRef();
        final BytesRef id = new BytesRef();

        protected abstract void doSearch(String id, Query matchQuery, Query highlight);

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
            queryCount++;
            doSearch(id.utf8ToString(), queries.getIfPresent(query.utf8ToString()), queries.getIfPresent(highlight.utf8ToString()));
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            this.queryDV = context.reader().getSortedDocValues(Monitor.FIELDS.query);
            this.highlightDV = context.reader().getSortedDocValues(Monitor.FIELDS.highlight);
            this.idDV = context.reader().getSortedDocValues(FIELDS.id);
        }

        @Override
        public boolean acceptsDocsOutOfOrder() {
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
