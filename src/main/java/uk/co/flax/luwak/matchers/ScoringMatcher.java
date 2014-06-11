package uk.co.flax.luwak.matchers;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import uk.co.flax.luwak.CandidateMatcher;
import uk.co.flax.luwak.InputDocument;
import uk.co.flax.luwak.MatcherFactory;

import java.io.IOException;
import java.util.*;

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

    private final Map<String, ScoringMatch> scores = new LinkedHashMap<>();

    public ScoringMatcher(InputDocument doc) {
        super(doc);
    }

    @Override
    public void matchQuery(String queryId, Query matchQuery, Query highlightQuery) throws IOException {
        float score = score(matchQuery);
        if (score > 0)
            scores.put(queryId, new ScoringMatch(queryId, score));
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
            searcher.search(query, new Collector() {

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

                @Override
                public void setNextReader(AtomicReaderContext context) { }
            });
            return scores[0];
        }
        catch (IOException e) {
            // Shouldn't happen, running on MemoryIndex...
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean matches(String queryId) {
        return scores.containsKey(queryId);
    }

    @Override
    public int getMatchCount() {
        return scores.size();
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

    @Override
    public Iterator<ScoringMatch> iterator() {
        return scores.values().iterator();
    }
}
