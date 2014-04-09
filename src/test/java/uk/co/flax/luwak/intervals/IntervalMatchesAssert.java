package uk.co.flax.luwak.intervals;

import org.fest.assertions.api.AbstractAssert;
import org.fest.assertions.api.Assertions;
import uk.co.flax.luwak.DocumentMatches;
import uk.co.flax.luwak.QueryMatch;

import static org.fest.assertions.api.Fail.fail;

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

public class IntervalMatchesAssert extends AbstractAssert<IntervalMatchesAssert, DocumentMatches> {

    protected IntervalMatchesAssert(DocumentMatches actual) {
        super(actual, IntervalMatchesAssert.class);
    }

    public QueryMatchAssert matchesQuery(String queryId) {
        for (QueryMatch match : actual.matches()) {
            if (match.getQueryId().equals(queryId))
                return new QueryMatchAssert((IntervalsQueryMatch) match);
        }
        fail("Document " + actual.docId() + " did not match query " + queryId);
        return null;
    }

    public static IntervalMatchesAssert assertThat(DocumentMatches actual) {
        return new IntervalMatchesAssert(actual);
    }

    public IntervalMatchesAssert matches(String docid) {
        Assertions.assertThat(actual.docId()).isEqualTo(docid);
        return this;
    }

    public IntervalMatchesAssert hasMatchCount(int count) {
        Assertions.assertThat(actual.matches()).hasSize(count);
        return this;
    }

    public IntervalMatchesAssert hasErrorCount(int count) {
        Assertions.assertThat(actual.errors()).hasSize(count);
        return this;
    }

    public IntervalMatchesAssert hasQueriesRunCount(int count) {
        Assertions.assertThat(actual.getMatchStats().querycount)
                .overridingErrorMessage("Expecting %d queries to be run, but was %d",
                        count, actual.getMatchStats().querycount)
                .isEqualTo(count);
        return this;
    }

    public IntervalMatchesAssert doesNotMatchQuery(String queryId) {
        for (QueryMatch match : actual.matches()) {
            Assertions.assertThat(match.getQueryId()).isNotEqualTo(queryId);
        }
        return this;
    }
}
