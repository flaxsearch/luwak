package uk.co.flax.luwak.matchers;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import uk.co.flax.luwak.CandidateMatcher;
import uk.co.flax.luwak.InputDocument;
import uk.co.flax.luwak.QueryMatch;

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

/**
 * Extend this class to create matches that require a Scorer
 *
 * @param <T> the type of QueryMatch that this class returns
 */
public abstract class CollectingMatcher<T extends QueryMatch> extends CandidateMatcher<T> {

    /**
     * Creates a new CollectingMatcher for the supplied InputDocument
     *
     * @param doc the document to run queries against
     */
    public CollectingMatcher(InputDocument doc) {
        super(doc);
    }

    @Override
    public T matchQuery(final String queryId, Query matchQuery, Query highlightQuery) throws IOException {

        MatchCollector coll = new MatchCollector(queryId);

        long t = System.nanoTime();
        doc.getSearcher().search(matchQuery, coll);
        t = System.nanoTime() - t;
        if (t > slowLogLimit)
            slowlog.append(queryId + ":" + (t / 1000000) + " ");

        if (coll.match != null)
            matches.put(queryId, coll.match);
        return coll.match;
    }

    /**
     * Called when a query matches the InputDocument
     * @param queryId the query ID
     * @param scorer the Scorer for this query
     * @return a match object
     * @throws IOException
     */
    protected abstract T doMatch(String queryId, Scorer scorer) throws IOException;

    protected class MatchCollector extends Collector {

        T match = null;

        private Scorer scorer;
        private final String queryId;

        public MatchCollector(String queryId) {
            this.queryId = queryId;
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            this.scorer = scorer;
        }

        @Override
        public void collect(int doc) throws IOException {
            match = doMatch(queryId, scorer);
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {

        }

        @Override
        public boolean acceptsDocsOutOfOrder() {
            return false;
        }
    }
}
