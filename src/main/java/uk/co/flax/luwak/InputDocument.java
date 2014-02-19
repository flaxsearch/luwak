package uk.co.flax.luwak;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;

import java.io.IOException;

/**
 * Copyright (c) 2013 Lemur Consulting Ltd.
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
 * An InputDocument represents a document to be run against registered queries
 * in the Monitor.  It should be constructed using the static #builder() method.
 */
public class InputDocument {

    /**
     * Create a new fluent {@link uk.co.flax.luwak.InputDocument.Builder} object.
     * @param id the id
     * @return a Builder
     */
    public static Builder builder(String id) {
        return new Builder(id);
    }

    public static Builder builder(String id, CollectorFactory factory) {
        return new Builder(id, factory);
    }

    private final String id;
    private final CollectorFactory collectorFactory;

    private final MemoryIndex index = new MemoryIndex(true);
    private IndexSearcher searcher;

    // protected constructor - use a Builder to create objects
    protected InputDocument(String id, CollectorFactory collectorFactory) {
        this.id = id;
        this.collectorFactory = collectorFactory;
    }

    private void finish() {
        searcher = index.createSearcher();
    }

    /**
     * Get the document's ID
     * @return the document's ID
     */
    public String getId() {
        return id;
    }

    /**
     * Run a MonitorQuery against this document.  If there are matches, and the MonitorQuery
     * has a non-null highlight query, the highlight query is then also run.  Matches are
     * returned from the highlight query, or from the original query if there are no highlight
     * matches.
     * @param mq the MonitorQuery to run
     * @return a {@link QueryMatch} object, or null if no matches are found.
     */
    public QueryMatch search(MonitorQuery mq) {

        QueryMatch matches = search(mq.getId(), "", mq.getQuery());
        if (matches == null)
            return null;

        QueryMatch highlightMatches = search(mq.getId(), "highlight ", mq.getHighlightQuery());
        if (highlightMatches == null)
            return matches;

        return highlightMatches;

    }

    private QueryMatch search(String id, String type, Query query) {
        if (query == null)
            return null;
        QueryMatchCollector mc = collectorFactory.createCollector(id);
        try {
            searcher.search(query, mc);
            return mc.getMatches();
        }
        catch (Exception e) {
            throw new RuntimeException("Error running " + type + "query " + id + " against document " + this.id, e);
        }
    }

    /**
     * Get an atomic reader over the internal index
     * @return an {@link org.apache.lucene.index.AtomicReader} over the internal index
     */
    public AtomicReader asAtomicReader() {
        return index.createSearcher().getIndexReader().leaves().get(0).reader();
    }

    /**
     * Fluent interface to construct a new InputDocument
     */
    public static class Builder {

        private final InputDocument doc;

        /**
         * Create a new Builder for an InputDocument with the given id
         * @param id the id of the InputDocument
         * @param factory a CollectorFactory used to create new QueryMatchCollector instances
         */
        public Builder(String id, CollectorFactory factory) {
            this.doc = new InputDocument(id, factory);
        }

        public Builder(String id) {
            this(id, new CollectorFactory());
        }

        /**
         * Add a field to the InputDocument
         * @param field the field name
         * @param text the text content of the field
         * @param analyzer the {@link Analyzer} to use for this field
         * @return the Builder object
         */
        public Builder addField(String field, String text, Analyzer analyzer) {
            doc.index.addField(field, text, analyzer);
            return this;
        }

        /**
         * Add a field to the InputDocument
         * @param field the field name
         * @param tokenStream a tokenstream containing the field contents
         * @return the Builder object
         */
        public Builder addField(String field, TokenStream tokenStream) {
            doc.index.addField(field, tokenStream);
            return this;
        }

        /**
         * Build the InputDocument
         * @return the InputDocument
         */
        public InputDocument build() {
            doc.finish();
            return doc;
        }

    }

    public abstract static class QueryMatchCollector extends Collector {

        public final String queryId;
        protected QueryMatch matches = null;

        public QueryMatchCollector(String queryId) {
            this.queryId = queryId;
        }

        public QueryMatch getMatches() {
            return matches;
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {

        }

        @Override
        public boolean acceptsDocsOutOfOrder() {
            return false;
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {

        }
    }

    static class DefaultMatchCollector extends QueryMatchCollector {

        public DefaultMatchCollector(String queryId) {
            super(queryId);
        }

        @Override
        public void collect(int doc) throws IOException {
            matches = new QueryMatch(queryId);
        }

    }

    public static class CollectorFactory {

        public QueryMatchCollector createCollector(String queryId) {
            return new DefaultMatchCollector(queryId);
        }

    }

}
