package uk.co.flax.luwak.assertions;

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;
import uk.co.flax.luwak.DocumentMatches;
import uk.co.flax.luwak.Matches;
import uk.co.flax.luwak.matchers.HighlightsMatch;

import static org.assertj.core.api.Fail.fail;


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

public class HighlightingMatchAssert extends AbstractAssert<HighlightingMatchAssert, Matches<HighlightsMatch>> {

    protected HighlightingMatchAssert(Matches<HighlightsMatch> actual) {
        super(actual, HighlightingMatchAssert.class);
    }

    public HighlightingMatchHitsAssert matchesQuery(String queryId, String docId) {
        for (HighlightsMatch match : actual.getMatches(docId)) {
            if (match.getQueryId().equals(queryId))
                return new HighlightingMatchHitsAssert(match, this);
        }
        fail("Document " + docId + " did not match query " + queryId);
        return null;
    }

    public static HighlightingMatchAssert assertThat(Matches<HighlightsMatch> actual) {
        return new HighlightingMatchAssert(actual);
    }

    public HighlightingMatchAssert hasMatchCount(String docId, int count) {
        Assertions.assertThat(actual.getMatchCount(docId)).isEqualTo(count);
        return this;
    }

    public HighlightingMatchAssert hasErrorCount(int count) {
        Assertions.assertThat(actual.getErrors()).hasSize(count);
        return this;
    }

    public HighlightingMatchAssert hasQueriesRunCount(int count) {
        Assertions.assertThat(actual.getQueriesRun())
                .overridingErrorMessage("Expecting %d queries to be run, but was %d",
                        count, actual.getQueriesRun())
                .isEqualTo(count);
        return this;
    }

    public HighlightingMatchAssert doesNotMatchQuery(String queryId, String docId) {
        DocumentMatches<HighlightsMatch> matches = actual.getMatches(docId);
        if (matches != null) {
            for (HighlightsMatch match : actual.getMatches(docId)) {
                Assertions.assertThat(match.getQueryId()).isNotEqualTo(queryId);
            }
        }
        return this;
    }
}
