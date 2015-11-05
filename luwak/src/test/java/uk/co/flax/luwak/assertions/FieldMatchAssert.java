package uk.co.flax.luwak.assertions;

import java.util.Collection;

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;
import uk.co.flax.luwak.matchers.HighlightsMatch;

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
public class FieldMatchAssert extends AbstractAssert<FieldMatchAssert, Collection<HighlightsMatch.Hit>> {

    private final HighlightingMatchHitsAssert parent;

    public FieldMatchAssert(HighlightingMatchHitsAssert parent, Collection<HighlightsMatch.Hit> actualHits) {
        super(actualHits, FieldMatchAssert.class);
        this.parent = parent;
    }

    public FieldMatchAssert withHit(HighlightsMatch.Hit hit) {
        Assertions.assertThat(actual).contains(hit);
        return this;
    }

    public FieldMatchAssert inField(String fieldname) {
        return parent.inField(fieldname);
    }

    public HighlightingMatchHitsAssert matchesQuery(String queryId, String docId) {
        return parent.parent.matchesQuery(queryId, docId);
    }

    public HighlightingMatchAssert doesNotMatchQuery(String queryId, String docId) {
        return parent.parent.doesNotMatchQuery(queryId, docId);
    }
}
