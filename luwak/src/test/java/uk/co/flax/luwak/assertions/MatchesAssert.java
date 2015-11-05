package uk.co.flax.luwak.assertions;

import com.google.common.collect.Lists;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;
import uk.co.flax.luwak.Matches;

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

public class MatchesAssert extends AbstractAssert<MatchesAssert, Matches<?>> {

    protected MatchesAssert(Matches<?> actual) {
        super(actual, MatchesAssert.class);
    }

    public static MatchesAssert assertThat(Matches<?> matches) {
        return new MatchesAssert(matches);
    }

    public MatchesAssert matchesQuery(String queryId, String docId) {
        Assertions.assertThat(actual.matches(queryId, docId))
                .overridingErrorMessage("Did not match query %s", queryId)
                .isNotNull();
        return this;
    }

    public MatchesAssert matchesDoc(String docid) {
        Assertions.assertThat(actual.getMatches(docid)).isNotEmpty();
        return this;
    }

    public MatchesAssert hasMatchCount(String docid, int count) {
        Assertions.assertThat(actual.getMatchCount(docid)).isEqualTo(count);
        return this;
    }

    public MatchesAssert hasQueriesRunCount(int count) {
        Assertions.assertThat(actual.getQueriesRun())
                .overridingErrorMessage("Expecting %d queries to be run, but was %d",
                        count, actual.getQueriesRun())
                .isEqualTo(count);
        return this;
    }

    public MatchesAssert selectedQuery(String queryId) {
        Assertions.assertThat(actual.getPresearcherHits().contains(queryId));
        return this;
    }

    public MatchesAssert selectedQueries(String queryId, String... queryIds) {
        Assertions.assertThat(actual.getPresearcherHits().containsAll(Lists.asList(queryId, queryIds)));
        return this;
    }

}
