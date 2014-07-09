package uk.co.flax.luwak.matchers;

import java.io.IOException;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SimpleCollector;
import uk.co.flax.luwak.CandidateMatcher;
import uk.co.flax.luwak.InputDocument;
import uk.co.flax.luwak.MatcherFactory;

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
 * A Matcher that reports the scores of queries run against its InputDocument
 */
public class ScoringMatcher extends CandidateMatcher<ScoringMatch> {

    public ScoringMatcher(InputDocument doc) {
        super(doc);
    }

    @Override
    public ScoringMatch doMatch(String queryId, Query matchQuery, Query highlightQuery) throws IOException {
        ScoringMatch match = null;
        float score = score(matchQuery);
        if (score > 0)
            match = new ScoringMatch(queryId, score);
        return match;
    }

    /**
     * Run a query against this matcher's InputDocument and report the score
     * @param query the query to run
     * @return the score
     */
    protected float score(Query query) {
        IndexSearcher searcher = doc.getSearcher();
        try {
            final float[] scores = new float[1]; // inits to 0.0f (no match)
            searcher.search(query, new SimpleCollector() {
                private Scorer scorer;

                @Override
                public void collect(int doc) throws IOException {
                    scores[0] = scorer.score();
                }

                @Override
                public void setScorer(Scorer scorer) {
                    this.scorer = scorer;
                }

                @Override
                public boolean acceptsDocsOutOfOrder() {
                    return true;
                }
            });
            return scores[0];
        }
        catch (IOException e) {
            // Shouldn't happen, running on MemoryIndex...
            throw new RuntimeException(e);
        }
    }

    /**
     * A MatcherFactory for ScoringMatcher objects
     */
    public static final MatcherFactory<ScoringMatcher> FACTORY = new MatcherFactory<ScoringMatcher>() {
        @Override
        public ScoringMatcher createMatcher(InputDocument doc) {
            return new ScoringMatcher(doc);
        }
    };

}
