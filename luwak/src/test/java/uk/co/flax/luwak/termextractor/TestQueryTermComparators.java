package uk.co.flax.luwak.termextractor;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.index.Term;
import org.junit.Test;
import uk.co.flax.luwak.termextractor.querytree.AnyNode;
import uk.co.flax.luwak.termextractor.querytree.ConjunctionNode;
import uk.co.flax.luwak.termextractor.querytree.DisjunctionNode;
import uk.co.flax.luwak.termextractor.querytree.QueryTree;
import uk.co.flax.luwak.termextractor.querytree.TermNode;
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

    @Test
    public void testAnyTokensAreNotPreferred() {

        QueryTree node1 = new TermNode(new QueryTerm("f", "foo", QueryTerm.Type.EXACT), 1.0);
        QueryTree node2 = new AnyNode("*:*");

        QueryTree conjunction = ConjunctionNode.build(node1, node2);
        Set<QueryTerm> terms = new HashSet<>();
        conjunction.collectTerms(terms);
        assertThat(terms).containsOnly(new QueryTerm("f", "foo", QueryTerm.Type.EXACT));
    }

    @Test
    public void testHigherWeightsArePreferred() {

        QueryTree node1 = new TermNode(new QueryTerm("f", "foo", QueryTerm.Type.EXACT), 1);
        QueryTree node2 = new TermNode(new QueryTerm("f", "foobar", QueryTerm.Type.EXACT), 1.5);

        QueryTree conjunction = ConjunctionNode.build(node1, node2);
        Set<QueryTerm> terms = new HashSet<>();
        conjunction.collectTerms(terms);
        assertThat(terms).containsOnly(new QueryTerm("f", "foobar", QueryTerm.Type.EXACT));
    }

    @Test
    public void testShorterTermListsArePreferred() {

        Term term = new Term("f", "foobar");

        QueryTree node1 = new TermNode(new QueryTerm(term), 1);
        QueryTree node2 = DisjunctionNode.build(new TermNode(new QueryTerm(term), 1),
                                                new TermNode(new QueryTerm(term), 1));

        QueryTree conjunction = ConjunctionNode.build(node1, node2);
        Set<QueryTerm> terms = new HashSet<>();
        conjunction.collectTerms(terms);
        assertThat(terms).hasSize(1);
    }

    @Test
    public void testFieldWeights() {

        TermWeightor weightor = new TermWeightor(new FieldWeightNorm(1.5f, "g"));
        assertThat(weightor.weigh(new QueryTerm("f", "foo", QueryTerm.Type.EXACT)))
                .isEqualTo(1);

        assertThat(weightor.weigh(new QueryTerm("g", "foo", QueryTerm.Type.EXACT)))
                .isEqualTo(1.5f);
    }

    @Test
    public void testTermWeights() {

        TermWeightor weight = new TermWeightor(new TermWeightNorm(0.01f, Collections.singleton("START")));
        assertThat(weight.weigh(new QueryTerm("f", "START", QueryTerm.Type.EXACT)))
                .isEqualTo(0.01f);
    }

    @Test
    public void testTermFrequencyNorms() {

        Map<String, Integer> termfreqs = ImmutableMap.of("france", 31635, "s", 47088);
        TermWeightor weight = new TermWeightor(new TermFrequencyWeightNorm(termfreqs, 100, 0.8f));

        assertThat(weight.weigh(new QueryTerm("f", "france", QueryTerm.Type.EXACT)))
                .isGreaterThan(weight.weigh(new QueryTerm("f", "s", QueryTerm.Type.EXACT)));

    }

    @Test
    public void testFieldSpecificTermWeightNorms() {

        TermWeightor weight = new TermWeightor(new FieldSpecificTermWeightNorm(0.1f, "field1", "f", "g"));

        assertThat(weight.weigh(new QueryTerm("field1", "f", QueryTerm.Type.EXACT)))
                .isEqualTo(0.1f);
        assertThat(weight.weigh(new QueryTerm("field2", "f", QueryTerm.Type.EXACT)))
                .isEqualTo(1);
    }

    @Test
    public void testTermTypeWeightNorms() {

        TermWeightor weight = new TermWeightor(new TermTypeNorm(QueryTerm.Type.CUSTOM, 0.2f),
                                                new TermTypeNorm(QueryTerm.Type.CUSTOM, "wildcard", 0.1f));

        assertThat(weight.weigh(new QueryTerm("field", "fooXX", QueryTerm.Type.CUSTOM, "wildcard")))
                .isEqualTo(0.1f);
        assertThat(weight.weigh(new QueryTerm("field", "foo", QueryTerm.Type.EXACT)))
                .isEqualTo(1);
        assertThat(weight.weigh(new QueryTerm("field", "foo", QueryTerm.Type.CUSTOM)))
                .isEqualTo(0.2f);
    }

}
