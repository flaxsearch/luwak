package uk.co.flax.luwak.matchers;

import org.apache.lucene.search.Query;
import uk.co.flax.luwak.InputDocument;
import uk.co.flax.luwak.MatcherFactory;
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
 * A delegating CandidateMatcher that reports the actual matching Query object along with
 * the delegate's QueryMatch
 *
 * @param <T> the QueryMatch type of the delegate
 */
public class QueryCacheingMatcher<T extends QueryMatch> extends DelegatingMatcher<T, QueryCacheingMatch<T>> {

    public QueryCacheingMatcher(InputDocument doc, MatcherFactory<T> factory) {
        super(doc, factory);
    }

    @Override
    protected QueryCacheingMatch<T> wrapMatch(T match, String queryId, Query matchQuery, Query highlightQuery) {
        return new QueryCacheingMatch<>(matchQuery, match);
    }

    @Override
    public QueryCacheingMatch<T> resolve(QueryCacheingMatch<T> match1, QueryCacheingMatch<T> match2) {
        return match1;
    }

    public static class QueryCacheingMatcherFactory<T extends QueryMatch> implements MatcherFactory<QueryCacheingMatch<T>> {

        private final MatcherFactory<T> matcherFactory;

        public QueryCacheingMatcherFactory(MatcherFactory<T> matcherFactory) {
            this.matcherFactory = matcherFactory;
        }

        @Override
        public QueryCacheingMatcher<T> createMatcher(InputDocument doc) {
            return new QueryCacheingMatcher<T>(doc, matcherFactory);
        }
    }

    public static <T extends QueryMatch> QueryCacheingMatcherFactory<T> factory(MatcherFactory<T> matcherFactory) {
        return new QueryCacheingMatcherFactory<>(matcherFactory);
    }
}
