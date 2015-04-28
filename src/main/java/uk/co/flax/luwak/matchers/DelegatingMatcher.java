package uk.co.flax.luwak.matchers;

import java.io.IOException;

import org.apache.lucene.search.Query;
import uk.co.flax.luwak.CandidateMatcher;
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

public abstract class DelegatingMatcher<M extends QueryMatch, W extends WrappedMatch<M>> extends CandidateMatcher<W> {

    private final CandidateMatcher<M> matcher;

    public DelegatingMatcher(InputDocument doc, MatcherFactory<M> factory) {
        super(doc);
        this.matcher = factory.createMatcher(doc);
    }

    @Override
    public W matchQuery(String queryId, Query matchQuery, Query highlightQuery) throws IOException {
        M match = matcher.matchQuery(queryId, matchQuery, highlightQuery);
        if (match == null)
            return wrapMiss(queryId, matchQuery, highlightQuery);

        W wrapped = wrapMatch(match, queryId, matchQuery, highlightQuery);
        this.addMatch(queryId, wrapped);
        return wrapped;
    }

    protected W wrapMiss(String queryId, Query matchQuery, Query highlightQuery) {
        return null;
    }

    protected abstract W wrapMatch(M match, String queryId, Query matchQuery, Query highlightQuery);
}
