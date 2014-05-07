package uk.co.flax.luwak.termextractor;

import com.google.common.collect.Lists;
import org.junit.Test;
import org.mockito.internal.util.collections.Sets;

import static org.fest.assertions.api.Assertions.assertThat;

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

public class TestQueryTermComparators {

    @Test
    public void testAnyTokensAreNotPreferred() {

        QueryTermList list1 = new QueryTermList(new QueryTerm("f", "foo", QueryTerm.Type.EXACT));
        QueryTermList list2 = new QueryTermList(new QueryTerm("f", "foo", QueryTerm.Type.WILDCARD));

        assertThat(QueryTermList.selectBest(Lists.newArrayList(list1, list2)))
                .containsExactly(new QueryTerm("f", "foo", QueryTerm.Type.EXACT));

    }

    @Test
    public void testUndesireableFieldsAreNotPreferred() {

        QueryTermList list1 = new QueryTermList(new QueryTerm("f", "foo", QueryTerm.Type.WILDCARD));
        QueryTermList list2 = new QueryTermList(new QueryTerm("g", "bar", QueryTerm.Type.EXACT));

        assertThat(QueryTermList.selectBest(Lists.newArrayList(list1, list2), Sets.newSet("g")))
                .containsExactly(new QueryTerm("f", "foo", QueryTerm.Type.WILDCARD));

    }

    @Test
    public void testUndesireableFieldsAreStillSelectedIfNecessary() {

        QueryTermList list = new QueryTermList(new QueryTerm("f", "foo", QueryTerm.Type.EXACT));
        assertThat(QueryTermList.selectBest(Lists.newArrayList(list, list), Sets.newSet("f")))
                .containsExactly(new QueryTerm("f", "foo", QueryTerm.Type.EXACT));

    }

}
