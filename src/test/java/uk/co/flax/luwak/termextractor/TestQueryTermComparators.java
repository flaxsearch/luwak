package uk.co.flax.luwak.termextractor;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.Test;
import org.mockito.internal.util.collections.Sets;
import uk.co.flax.luwak.termextractor.weights.*;

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

    private static TermWeightor WEIGHT = new ReportingWeightor(CompoundRuleWeightor.newWeightor().build());

    @Test
    public void testAnyTokensAreNotPreferred() {

        List<QueryTerm> list1 = ImmutableList.of(new QueryTerm("f", "foo", QueryTerm.Type.EXACT));
        List<QueryTerm> list2 = ImmutableList.of(new QueryTerm("f", "foo", QueryTerm.Type.WILDCARD));

        assertThat(WEIGHT.selectBest(Lists.newArrayList(list1, list2)))
                .containsExactly(new QueryTerm("f", "foo", QueryTerm.Type.EXACT));

    }

    @Test
    public void testLongerTokensArePreferred() {

        List<QueryTerm> list1 = ImmutableList.of(new QueryTerm("f", "foo", QueryTerm.Type.EXACT));
        List<QueryTerm> list2 = ImmutableList.of(new QueryTerm("f", "foobar", QueryTerm.Type.EXACT));

        assertThat(WEIGHT.selectBest(Lists.newArrayList(list1, list2)))
                .containsExactly(new QueryTerm("f", "foobar", QueryTerm.Type.EXACT));

    }

    @Test
    public void testShorterTermListsArePreferred() {

        List<QueryTerm> list1 = ImmutableList.of(new QueryTerm("f", "foobar", QueryTerm.Type.EXACT));
        List<QueryTerm> list2 = ImmutableList.of(new QueryTerm("f", "foobar", QueryTerm.Type.EXACT),
                new QueryTerm("f", "foobar", QueryTerm.Type.EXACT));

        assertThat(WEIGHT.selectBest(Lists.newArrayList(list1, list2)))
                .containsExactly(new QueryTerm("f", "foobar", QueryTerm.Type.EXACT));

        List<QueryTerm> emptyList = ImmutableList.of();
        assertThat(WEIGHT.selectBest(Lists.newArrayList(list1, emptyList)))
                .containsExactly(new QueryTerm("f", "foobar", QueryTerm.Type.EXACT));
    }

    @Test
    public void testUndesireableFieldsAreNotPreferred() {

        TermWeightor weight = CompoundRuleWeightor.newWeightor()
                .withRule(new FieldWeightRule(Sets.newSet("g"), 0.7f))
                .build();

        List<QueryTerm> list1 = ImmutableList.of(new QueryTerm("f", "foo", QueryTerm.Type.WILDCARD));
        List<QueryTerm> list2 = ImmutableList.of(new QueryTerm("g", "bar", QueryTerm.Type.EXACT));

        assertThat(weight.selectBest(Lists.newArrayList(list1, list2)))
                .containsExactly(new QueryTerm("f", "foo", QueryTerm.Type.WILDCARD));

    }

    @Test
    public void testUndesireableFieldsAreStillSelectedIfNecessary() {

        TermWeightor weight = CompoundRuleWeightor.newWeightor()
                .withRule(new FieldWeightRule(Sets.newSet("f"), 0.7f)).build();

        List<QueryTerm> list = ImmutableList.of(new QueryTerm("f", "foo", QueryTerm.Type.EXACT));
        assertThat(weight.selectBest(Lists.newArrayList(list, list)))
                .containsExactly(new QueryTerm("f", "foo", QueryTerm.Type.EXACT));

    }

    @Test
    public void testUndesirableTokensAreNotPreferred() {

        Map<String, Float> termweights = ImmutableMap.of("START", 0.01f);
        TermWeightor weight = CompoundRuleWeightor.newWeightor()
                .withRule(new TermWeightRule(termweights)).build();

        List<QueryTerm> list1 = ImmutableList.of(new QueryTerm("f", "START", QueryTerm.Type.EXACT));
        List<QueryTerm> list2 = ImmutableList.of(new QueryTerm("f", "a", QueryTerm.Type.EXACT));

        assertThat(weight.selectBest(Lists.newArrayList(list1, list2)))
                .containsExactly(new QueryTerm("f", "a", QueryTerm.Type.EXACT));
    }

    @Test
    public void testTermFrequencyNorms() {

        Map<String, Integer> termfreqs = ImmutableMap.of("wibble", 100);
        TermWeightor weight = new ReportingWeightor(CompoundRuleWeightor.newWeightor()
                .withRule(new TermFrequencyWeightRule(termfreqs, 1, 0.02f)).build());

        List<QueryTerm> list1 = ImmutableList.of(new QueryTerm("f", "wibble", QueryTerm.Type.EXACT));
        List<QueryTerm> list2 = ImmutableList.of(new QueryTerm("f", "quack", QueryTerm.Type.EXACT));

        assertThat(weight.selectBest(Lists.newArrayList(list1, list2)))
                .containsExactly(new QueryTerm("f", "quack", QueryTerm.Type.EXACT));

    }

}
