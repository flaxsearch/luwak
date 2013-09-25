package uk.co.flax.luwak.util;

import org.fest.assertions.api.AbstractAssert;
import org.fest.assertions.api.Assertions;
import uk.co.flax.luwak.QueryMatch;

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
public class QueryMatchAssert extends AbstractAssert<QueryMatchAssert, QueryMatch> {

    protected QueryMatchAssert(QueryMatch actual) {
        super(actual, QueryMatchAssert.class);
    }

    public static QueryMatchAssert assertThat(QueryMatch actual) {
        return new QueryMatchAssert(actual);
    }

    public QueryMatchAssert withHit(QueryMatch.Hit hit) {
        Assertions.assertThat(actual.getHits()).contains(hit);
        return this;
    }

    public QueryMatchAssert withHitCount(int count) {
        Assertions.assertThat(actual.getHits()).hasSize(count);
        return this;
    }
}
