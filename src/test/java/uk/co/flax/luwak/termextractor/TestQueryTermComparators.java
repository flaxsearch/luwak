package uk.co.flax.luwak.termextractor;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.index.Term;
import org.junit.Test;
import org.mockito.internal.util.collections.Sets;
import uk.co.flax.luwak.termextractor.querytree.ConjunctionNode;
import uk.co.flax.luwak.termextractor.querytree.QueryTree;
import uk.co.flax.luwak.termextractor.querytree.TermNode;
import uk.co.flax.luwak.termextractor.querytree.TreeWeightor;
import uk.co.flax.luwak.termextractor.weights.*;

import static org.assertj.core.api.Assertions.assertThat;

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

    private static TreeWeightor WEIGHT = new ReportingWeightor(TreeWeightor.DEFAULT_WEIGHTOR);

    @Test
    public void testAnyTokensAreNotPreferred() {

        QueryTree node1 = new TermNode(new QueryTerm("f", "foo", QueryTerm.Type.EXACT));
        QueryTree node2 = new TermNode(new QueryTerm("f", "foo", QueryTerm.Type.ANY));

        assertThat(WEIGHT.select(Sets.newSet(node1, node2)))
                .isSameAs(node1);

    }

    @Test
    public void testLongerTokensArePreferred() {

        QueryTree node1 = new TermNode(new QueryTerm("f", "foo", QueryTerm.Type.EXACT));
        QueryTree node2 = new TermNode(new QueryTerm("f", "foobar", QueryTerm.Type.EXACT));

        assertThat(WEIGHT.select(Sets.newSet(node1, node2)))
                .isSameAs(node2);

    }

    @Test
    public void testShorterTermListsArePreferred() {

        Term term = new Term("f", "foobar");

        QueryTree node1 = new TermNode(new QueryTerm(term));
        QueryTree node2 = ConjunctionNode.build(new TermNode(new QueryTerm(term)),
                                                new TermNode(new QueryTerm(term)));

        assertThat(WEIGHT.select(Sets.newSet(node1, node2)))
                .isSameAs(node1);
    }

    @Test
    public void testUndesireableFieldsAreNotPreferred() {

        TreeWeightor weight = new TreeWeightor(new FieldWeightNorm(0.7f,  "g"));

        QueryTree node1 = new TermNode(new QueryTerm("f", "foo", QueryTerm.Type.ANY));
        QueryTree node2 = new TermNode(new QueryTerm("g", "bar", QueryTerm.Type.EXACT));

        assertThat(weight.select(Sets.newSet(node1, node2)))
                .isSameAs(node1);

    }

    @Test
    public void testUndesireableFieldsAreStillSelectedIfNecessary() {

        TreeWeightor weight = new TreeWeightor(new FieldWeightNorm(0.7f, "f"));

        QueryTree node1 = new TermNode(new QueryTerm("f", "foo", QueryTerm.Type.EXACT));
        assertThat(weight.select(Sets.newSet(node1)))
                .isSameAs(node1);

    }

    @Test
    public void testUndesirableTokensAreNotPreferred() {

        Map<String, Float> termweights = ImmutableMap.of("START", 0.01f);
        TreeWeightor weight = new TreeWeightor(new TermWeightPolicy(termweights));

        QueryTree node1 = new TermNode(new QueryTerm("f", "START", QueryTerm.Type.EXACT));
        QueryTree node2 = new TermNode(new QueryTerm("f", "a", QueryTerm.Type.EXACT));

        assertThat(weight.select(Sets.newSet(node1, node2)))
                .isSameAs(node2);
    }

    @Test
    public void testTermFrequencyNorms() {

        Map<String, Integer> termfreqs = ImmutableMap.of("france", 31635, "s", 47088);
        TreeWeightor weight = new TreeWeightor(new TermFrequencyWeightPolicy(termfreqs, 100, 0.8f));

        QueryTree node1 = new TermNode(new QueryTerm("f", "france", QueryTerm.Type.EXACT));
        QueryTree node2 = new TermNode(new QueryTerm("f", "s", QueryTerm.Type.EXACT));

        assertThat(weight.select(Sets.newSet(node1, node2)))
                .isSameAs(node1);

    }

    @Test
    public void testTermWeightNorms() {

        TreeWeightor weight = new TreeWeightor(new TermWeightNorm(0.1f, "f"));

        QueryTree node1 = new TermNode(new QueryTerm("f", "f", QueryTerm.Type.EXACT));
        QueryTree node2 = new TermNode(new QueryTerm("f", "g", QueryTerm.Type.EXACT));
        assertThat(weight.select(Sets.newSet(node1, node2)))
                .isSameAs(node2);

    }

    @Test
    public void testFieldSpecificTermWeightNorms() {

        TreeWeightor weight = new TreeWeightor(new FieldSpecificTermWeightNorm(0.1f, "field1", "f", "g"));

        QueryTree node1 = new TermNode(new QueryTerm("field2", "f", QueryTerm.Type.EXACT));
        QueryTree node2 = new TermNode(new QueryTerm("field1", "f", QueryTerm.Type.EXACT));
        QueryTree node3 = new TermNode(new QueryTerm("field1", "g", QueryTerm.Type.EXACT));

        assertThat(weight.select(Sets.newSet(node1, node2, node3)))
                .isSameAs(node1);
    }

    @Test
    public void testTermTypeWeightNorms() {

        Map<String, Integer> termFreqs = ImmutableMap.of(
                "foo", 400, "bar", 4000
        );
        TreeWeightor weight = new TreeWeightor(new TermFrequencyWeightPolicy(termFreqs, 100, 0,
                                                new TermTypeNorm(QueryTerm.Type.CUSTOM, 0.2f),
                                                new TermTypeNorm(QueryTerm.Type.CUSTOM, "wildcard", 0.1f)));

        //weight = new ReportingWeightor(weight);

        QueryTree node1 = new TermNode(new QueryTerm("field", "foo", QueryTerm.Type.EXACT));
        QueryTree node2 = new TermNode(new QueryTerm("field", "fooXX", QueryTerm.Type.CUSTOM, "wildcard"));
        QueryTree node3 = new TermNode(new QueryTerm("field", "wibble", QueryTerm.Type.CUSTOM));
        QueryTree node4 = new TermNode(new QueryTerm("field", "bar", QueryTerm.Type.EXACT));

        assertThat(weight.select(Sets.newSet(node1, node2, node3, node4)))
                .isSameAs(node1);

        assertThat(weight.select(Sets.newSet(node2, node3)))
                .isSameAs(node3);

        assertThat(weight.select(Sets.newSet(node2, node4)))
                .isSameAs(node2);
    }

}
