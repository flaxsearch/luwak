package uk.co.flax.luwak.matchers;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.*;
import uk.co.flax.luwak.CandidateMatcher;
import uk.co.flax.luwak.DocumentBatch;
import uk.co.flax.luwak.QueryMatch;

/*
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

/**
 * Extend this class to create matches that require a Scorer
 *
 * @param <T> the type of QueryMatch that this class returns
 */
public abstract class CollectingMatcher<T extends QueryMatch> extends CandidateMatcher<T> {

    /**
     * Creates a new CollectingMatcher for the supplied DocumentBatch
     *
     * @param docs the documents to run queries against
     */
    public CollectingMatcher(DocumentBatch docs) {
        super(docs);
    }

    @Override
    protected void doMatchQuery(final String queryId, Query matchQuery, Map<String, String> metadata) throws IOException {

        MatchCollector coll = buildMatchCollector(queryId);

        long t = System.nanoTime();
        IndexSearcher searcher = docs.getSearcher();
        searcher.search(matchQuery, coll);
        t = System.nanoTime() - t;
        this.slowlog.addQuery(queryId, t);

    }

    protected MatchCollector buildMatchCollector(String queryId) {
        return new MatchCollector(queryId);
    }

    /**
     * Called when a query matches an InputDocument
     * @param queryId the query ID
     * @param docId the docId for the InputDocument in the DocumentBatch
     * @param scorer the Scorer for this query
     * @return a match object
     * @throws IOException on IO error
     */
    protected abstract T doMatch(String queryId, String docId, Scorer scorer) throws IOException;

    protected class MatchCollector implements Collector {

        T match = null;

        private Scorer scorer;
        private final String queryId;

        public MatchCollector(String queryId) {
            this.queryId = queryId;
        }


        @Override
        public LeafCollector getLeafCollector(LeafReaderContext leafReaderContext) throws IOException {
            return new MatchLeafCollector();
        }

        @Override
        public boolean needsScores() {
            return true;
        }

        public class MatchLeafCollector implements LeafCollector {

            @Override
            public void setScorer(Scorer scorer) throws IOException {
                MatchCollector.this.scorer = scorer;
            }

            @Override
            public void collect(int doc) throws IOException {
                match = doMatch(queryId, docs.resolveDocId(doc), scorer);
                if (match != null)
                    addMatch(match);
            }

        }
    }
}
