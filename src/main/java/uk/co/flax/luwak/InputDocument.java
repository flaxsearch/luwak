package uk.co.flax.luwak;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.*;
import org.apache.lucene.search.intervals.Interval;
import org.apache.lucene.search.intervals.IntervalCollector;
import org.apache.lucene.search.intervals.IntervalIterator;

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

    private ExceptionHandler exceptionHandler;

    /**
     * Create a new fluent {@link uk.co.flax.luwak.InputDocument.Builder} object.
     * @param id the id
     * @return a Builder
     */
    public static Builder builder(String id) {
        return new Builder(id);
    }

    private final String id;

    private final MemoryIndex index = new MemoryIndex(true);
    private IndexSearcher searcher;

    // private constructor - use a Builder to create objects
    private InputDocument(String id) {
        this.id = id;
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
        QueryMatchCollector mc = new QueryMatchCollector(id);
        try {
            searcher.search(query, mc);
            return mc.getMatches();
        }
        catch (Exception e) {
            if (exceptionHandler == null) {
                throw new RuntimeException("Error running " + type + "query " + id + " against document " + this.id, e);
            } else {
                exceptionHandler.exception(e);
            }
        }
        return null;
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
         */
        public Builder(String id) {
            this.doc = new InputDocument(id);
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

        public Builder setExceptionHandler(ExceptionHandler handler) {
            doc.exceptionHandler = handler;
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

    // a specialized Collector that uses an {@link IntervalIterator} to collect
    // match positions from a Scorer.
    static class QueryMatchCollector extends Collector implements IntervalCollector {

        protected Scorer scorer;
        private IntervalIterator positions;

        private QueryMatch matches = null;
        private final String queryId;

        public QueryMatchCollector(String queryId) {
            this.queryId = queryId;
        }

        public QueryMatch getMatches() {
            return matches;
        }

        @Override
        public void collect(int doc) throws IOException {
            // consume any remaining positions the scorer didn't report
            matches = new QueryMatch(this.queryId);
            positions.scorerAdvanced(doc);
            while(positions.next() != null) {
                positions.collect(this);
            }
        }

        public boolean acceptsDocsOutOfOrder() {
            return false;
        }

        public void setScorer(Scorer scorer) throws IOException {
            this.scorer = scorer;
            positions = scorer.intervals(true);
            // If we want to visit the other scorers, we can, here...
        }

        public void setNextReader(AtomicReaderContext context) throws IOException {
        }

        @Override
        public Weight.PostingFeatures postingFeatures() {
            return Weight.PostingFeatures.OFFSETS;
        }

        @Override
        public void collectLeafPosition(Scorer scorer, Interval interval, int docID) {
            matches.addInterval(interval);
        }

        @Override
        public void collectComposite(Scorer scorer, Interval interval,
                                     int docID) {
            //offsets.add(new Offset(interval.begin, interval.end, interval.offsetBegin, interval.offsetEnd));
        }

    }
}
