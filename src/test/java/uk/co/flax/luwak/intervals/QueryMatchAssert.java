package uk.co.flax.luwak.intervals;

import org.fest.assertions.api.AbstractAssert;
import org.fest.assertions.api.Assertions;

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
public class QueryMatchAssert extends AbstractAssert<QueryMatchAssert, IntervalsQueryMatch> {

    protected QueryMatchAssert(IntervalsQueryMatch actual) {
        super(actual, QueryMatchAssert.class);
    }

    public static QueryMatchAssert assertThat(IntervalsQueryMatch actual) {
        return new QueryMatchAssert(actual);
    }

    public QueryMatchAssert withHitCount(int count) {
        Assertions.assertThat(actual.getHitCount()).isEqualTo(count);
        return this;
    }

    public FieldMatchAssert inField(String fieldname) {
        Assertions.assertThat(actual.getHits(fieldname).size()).isGreaterThan(0);
        return new FieldMatchAssert(this, actual.getHits(fieldname));
    }
}
