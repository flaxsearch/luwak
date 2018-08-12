package uk.co.flax.luwak.termextractor;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.junit.Test;
import uk.co.flax.luwak.termextractor.querytree.AnyNode;
import uk.co.flax.luwak.termextractor.querytree.ConjunctionNode;
import uk.co.flax.luwak.termextractor.querytree.DisjunctionNode;
import uk.co.flax.luwak.termextractor.querytree.QueryTree;
import uk.co.flax.luwak.termextractor.querytree.QueryTreeViewer;
import uk.co.flax.luwak.termextractor.querytree.TermNode;
import uk.co.flax.luwak.termextractor.weights.TermWeightNorm;
import uk.co.flax.luwak.termextractor.weights.TermWeightor;
import uk.co.flax.luwak.termextractor.weights.TokenLengthNorm;
import uk.co.flax.luwak.testutils.ParserUtils;

import static junit.framework.TestCase.assertTrue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;

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

public class TestQueryAnalyzer {

    public static final QueryAnalyzer analyzer = new QueryAnalyzer();
    private static final TermWeightor WEIGHTOR = new TermWeightor(new TokenLengthNorm());

    @Test
    public void testAdvancesCollectDifferentTerms() throws Exception {

        Query q = ParserUtils.parse("field:(+hello +goodbye)");
        QueryTree querytree = analyzer.buildTree(q, WEIGHTOR);

        assertThat(analyzer.collectTerms(querytree))
                .containsExactly(new QueryTerm("field", "goodbye", QueryTerm.Type.EXACT));

        assertThat(querytree.advancePhase(0)).isTrue();

        assertThat(analyzer.collectTerms(querytree))
                .containsExactly(new QueryTerm("field", "hello", QueryTerm.Type.EXACT));

        assertThat(querytree.advancePhase(0)).isFalse();

        assertThat(analyzer.collectTerms(querytree))
                .containsExactly(new QueryTerm("field", "hello", QueryTerm.Type.EXACT));

    }

    @Test
    public void testDisjunctionsWithAnyClausesOnlyReturnANYTOKEN() throws Exception {

        // disjunction containing a pure negative - we can't narrow this down
        Query q = ParserUtils.parse("hello goodbye (*:* -term)");

        assertThat(analyzer.collectTerms(q, WEIGHTOR))
                .extracting("type")
                .containsOnly(QueryTerm.Type.ANY);

    }

    @Test
    public void testConjunctionsDoNotAdvanceOverANYTOKENs() throws Exception {

        Query q = ParserUtils.parse("+hello +howdyedo +(goodbye (*:* -whatever))");
        QueryTree tree = analyzer.buildTree(q, WEIGHTOR);

        assertThat(analyzer.collectTerms(tree))
                .containsOnly(new QueryTerm("field", "howdyedo", QueryTerm.Type.EXACT));
        assertThat(tree.advancePhase(0)).isTrue();
        assertThat(analyzer.collectTerms(tree))
                .containsOnly(new QueryTerm("field", "hello", QueryTerm.Type.EXACT));
        QueryTreeViewer.view(tree, System.out);
        assertThat(tree.advancePhase(0)).isFalse();
        assertThat(analyzer.collectTerms(tree))
                .containsOnly(new QueryTerm("field", "hello", QueryTerm.Type.EXACT));

    }

    @Test
    public void testConjunctionsCannotAdvanceOverZeroWeightedTokens() throws Exception {

        TermWeightor weightor = new TermWeightor(new TermWeightNorm(0, "startterm"), new TokenLengthNorm(1, 1));

        QueryAnalyzer analyzer = new QueryAnalyzer();

        Query q = ParserUtils.parse("+startterm +hello +goodbye");
        QueryTree tree = analyzer.buildTree(q, weightor);

        QueryTreeViewer.view(tree, System.out);

        assertThat(analyzer.collectTerms(tree))
                .containsOnly(new QueryTerm("field", "goodbye", QueryTerm.Type.EXACT));
        assertThat(tree.advancePhase(0))
                .isTrue();
        assertThat(analyzer.collectTerms(tree))
                .containsOnly(new QueryTerm("field", "hello", QueryTerm.Type.EXACT));
        QueryTreeViewer.view(tree, System.out);
        assertThat(tree.advancePhase(0))
                .isFalse();
        QueryTreeViewer.view(tree, System.out);

    }

    @Test
    public void testNestedConjunctions() throws Exception {

        Query q = ParserUtils.parse("+(+(+(+aaaa +cc) +(+d +bbb)))");
        QueryTree tree = analyzer.buildTree(q, WEIGHTOR);

        assertThat(analyzer.collectTerms(tree))
                .containsOnly(new QueryTerm("field", "aaaa", QueryTerm.Type.EXACT));
        assertThat(tree.advancePhase(0))
                .isTrue();

        assertThat(analyzer.collectTerms(tree))
                .containsOnly(new QueryTerm("field", "bbb", QueryTerm.Type.EXACT));
        assertThat(tree.advancePhase(0))
                .isTrue();

        assertThat(analyzer.collectTerms(tree))
                .containsOnly(new QueryTerm("field", "cc", QueryTerm.Type.EXACT));
        assertThat(tree.advancePhase(0))
                .isTrue();

        assertThat(analyzer.collectTerms(tree))
                .containsOnly(new QueryTerm("field", "d", QueryTerm.Type.EXACT));
        assertThat(tree.advancePhase(0))
                .isFalse();

    }

    @Test
    public void testNestedDisjunctions() throws Exception {

        Query q = ParserUtils.parse("+(+((+aaaa +cc) (+dd +bbb +f)))");
        QueryTree tree = analyzer.buildTree(q, WEIGHTOR);

        assertThat(analyzer.collectTerms(tree))
                .containsOnly(new QueryTerm("field", "aaaa", QueryTerm.Type.EXACT),
                              new QueryTerm("field", "bbb", QueryTerm.Type.EXACT));

        assertThat(tree.advancePhase(0)).isTrue();
        assertThat(analyzer.collectTerms(tree))
                .containsOnly(new QueryTerm("field", "cc", QueryTerm.Type.EXACT),
                              new QueryTerm("field", "dd", QueryTerm.Type.EXACT));

        assertThat(tree.advancePhase(0)).isTrue();
        assertThat(analyzer.collectTerms(tree))
                .containsOnly(new QueryTerm("field", "cc", QueryTerm.Type.EXACT),
                        new QueryTerm("field", "f", QueryTerm.Type.EXACT));

        assertThat(tree.advancePhase(0)).isFalse();
    }

    @Test
    public void testMinWeightAdvances() {
        QueryTree tree = DisjunctionNode.build(
                ConjunctionNode.build(
                        new TermNode(new QueryTerm("field", "term1", QueryTerm.Type.EXACT), 1),
                        new TermNode(new QueryTerm("field", "term2", QueryTerm.Type.EXACT), 0.1),
                        new AnyNode("*:*")
                ),
                ConjunctionNode.build(
                        DisjunctionNode.build(
                                new TermNode(new QueryTerm("field", "term4", QueryTerm.Type.EXACT), 0.2),
                                new TermNode(new QueryTerm("field", "term5", QueryTerm.Type.EXACT), 1)
                        ),
                        new TermNode(new QueryTerm("field", "term3", QueryTerm.Type.EXACT), 0.5)
                )
        );

        assertThat(analyzer.collectTerms(tree))
                .extracting("term")
                .containsOnly(new Term("field", "term1"), new Term("field", "term3"));

        assertTrue(tree.advancePhase(0.1f));
        assertThat(analyzer.collectTerms(tree))
                .extracting("term")
                .containsOnly(new Term("field", "term1"), new Term("field", "term4"), new Term("field", "term5"));

        assertFalse(tree.advancePhase(0.1f));
    }

}
