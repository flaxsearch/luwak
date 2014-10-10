package uk.co.flax.luwak.matchers;

import java.io.IOException;

import org.apache.lucene.search.Query;
import uk.co.flax.luwak.CandidateMatcher;
import uk.co.flax.luwak.InputDocument;
import uk.co.flax.luwak.MatcherFactory;
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
 * A delegating CandidateMatcher that reports the actual matching Query object along with
 * the delegate's QueryMatch
 *
 * @param <T> the QueryMatch type of the delegate
 */
public class QueryCacheingMatcher<T extends QueryMatch> extends CandidateMatcher<QueryCacheingMatch<T>> {

    private final CandidateMatcher<T> matcher;

    public QueryCacheingMatcher(InputDocument doc, MatcherFactory<? extends CandidateMatcher<T>> factory) {
        super(doc);
        this.matcher = factory.createMatcher(doc);
    }

    @Override
    public QueryCacheingMatch<T> matchQuery(String queryId, Query matchQuery, Query highlightQuery) throws IOException {
        T match = this.matcher.matchQuery(queryId, matchQuery, highlightQuery);
        if (match == null)
            return null;
        QueryCacheingMatch<T> m = new QueryCacheingMatch<>(queryId, matchQuery, match);
        this.addMatch(queryId, m);
        return m;
    }

    public static class QueryCacheingMatcherFactory<T extends QueryMatch> implements MatcherFactory<QueryCacheingMatcher<T>> {

        private final MatcherFactory<? extends CandidateMatcher<T>> matcherFactory;

        public QueryCacheingMatcherFactory(MatcherFactory<? extends CandidateMatcher<T>> matcherFactory) {
            this.matcherFactory = matcherFactory;
        }

        @Override
        public QueryCacheingMatcher<T> createMatcher(InputDocument doc) {
            return new QueryCacheingMatcher<>(doc, matcherFactory);
        }
    }

    public static <T extends QueryMatch> QueryCacheingMatcherFactory<T> factory(MatcherFactory<? extends CandidateMatcher<T>> matcherFactory) {
        return new QueryCacheingMatcherFactory<>(matcherFactory);
    }
}
