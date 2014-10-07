package uk.co.flax.luwak.termextractor;

import org.apache.lucene.search.Query;
import org.junit.Test;
import uk.co.flax.luwak.termextractor.querytree.TreeAdvancer;
import uk.co.flax.luwak.termextractor.querytree.QueryTree;
import uk.co.flax.luwak.termextractor.querytree.QueryTreeViewer;
import uk.co.flax.luwak.termextractor.querytree.TreeWeightor;
import uk.co.flax.luwak.termextractor.weights.TermWeightNorm;
import uk.co.flax.luwak.termextractor.weights.TokenLengthNorm;
import uk.co.flax.luwak.util.ParserUtils;

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

public class TestQueryAnalyzer {

    public static final QueryAnalyzer analyzer = new QueryAnalyzer();

    public static final TreeAdvancer advancer = new TreeAdvancer.MinWeightTreeAdvancer(analyzer.weightor, 0);

    @Test
    public void testAdvancesCollectDifferentTerms() throws Exception {

        Query q = ParserUtils.parse("field:(+hello +goodbye)");
        QueryTree querytree = analyzer.buildTree(q);

        assertThat(analyzer.collectTerms(querytree))
                .containsExactly(new QueryTerm("field", "goodbye", QueryTerm.Type.EXACT));

        assertThat(analyzer.advancePhase(querytree, advancer)).isTrue();

        assertThat(analyzer.collectTerms(querytree))
                .containsExactly(new QueryTerm("field", "hello", QueryTerm.Type.EXACT));

        assertThat(analyzer.advancePhase(querytree, advancer)).isFalse();

        assertThat(analyzer.collectTerms(querytree))
                .containsExactly(new QueryTerm("field", "hello", QueryTerm.Type.EXACT));

    }

    @Test
    public void testDisjunctionsWithAnyClausesOnlyReturnANYTOKEN() throws Exception {

        // disjunction containing a pure negative - we can't narrow this down
        Query q = ParserUtils.parse("hello goodbye (*:* -term)");

        assertThat(analyzer.collectTerms(q))
                .containsOnly(new QueryTerm("", "DISJUNCTION WITH ANYTOKEN", QueryTerm.Type.ANY));

    }

    @Test
    public void testConjunctionsCannotAdvanceOverANYTOKENs() throws Exception {

        Query q = ParserUtils.parse("+hello +howdyedo +(goodbye (*:* -whatever))");
        QueryTree tree = analyzer.buildTree(q);

        assertThat(analyzer.collectTerms(tree))
                .containsOnly(new QueryTerm("field", "howdyedo", QueryTerm.Type.EXACT));
        assertThat(analyzer.advancePhase(tree, advancer)).isTrue();
        assertThat(analyzer.collectTerms(tree))
                .containsOnly(new QueryTerm("field", "hello", QueryTerm.Type.EXACT));
        QueryTreeViewer.view(tree, analyzer.weightor, advancer, System.out);
        assertThat(analyzer.advancePhase(tree, advancer)).isFalse();
        assertThat(analyzer.collectTerms(tree))
                .containsOnly(new QueryTerm("field", "hello", QueryTerm.Type.EXACT));

    }

    @Test
    public void testConjunctionsCannotAdvanceOverZeroWeightedTokens() throws Exception {

        TreeWeightor weightor = new TreeWeightor(new TermWeightNorm(0, "startterm"), new TokenLengthNorm(1, 1));

        QueryAnalyzer analyzer = new QueryAnalyzer(weightor);
        TreeAdvancer advancer = new TreeAdvancer.MinWeightTreeAdvancer(weightor, 0);

        Query q = ParserUtils.parse("+startterm +hello +goodbye");
        QueryTree tree = analyzer.buildTree(q);

        QueryTreeViewer.view(tree, analyzer.weightor, advancer, System.out);

        assertThat(analyzer.collectTerms(tree))
                .containsOnly(new QueryTerm("field", "goodbye", QueryTerm.Type.EXACT));
        assertThat(analyzer.advancePhase(tree, advancer))
                .isTrue();
        assertThat(analyzer.collectTerms(tree))
                .containsOnly(new QueryTerm("field", "hello", QueryTerm.Type.EXACT));
        QueryTreeViewer.view(tree, analyzer.weightor, advancer, System.out);
        assertThat(analyzer.advancePhase(tree, advancer))
                .isFalse();
        QueryTreeViewer.view(tree, analyzer.weightor, advancer, System.out);

    }

    @Test
    public void testNestedConjunctions() throws Exception {

        TreeAdvancer advancer = new TreeAdvancer.MinWeightTreeAdvancer(analyzer.weightor, 0);

        Query q = ParserUtils.parse("+(+(+(+aaaa +cc) +(+d +bbb)))");
        QueryTree tree = analyzer.buildTree(q);

        assertThat(analyzer.collectTerms(tree))
                .containsOnly(new QueryTerm("field", "aaaa", QueryTerm.Type.EXACT));
        assertThat(analyzer.advancePhase(tree, advancer))
                .isTrue();

        assertThat(analyzer.collectTerms(tree))
                .containsOnly(new QueryTerm("field", "bbb", QueryTerm.Type.EXACT));
        assertThat(analyzer.advancePhase(tree, advancer))
                .isTrue();

        assertThat(analyzer.collectTerms(tree))
                .containsOnly(new QueryTerm("field", "cc", QueryTerm.Type.EXACT));
        assertThat(analyzer.advancePhase(tree, advancer))
                .isTrue();

        assertThat(analyzer.collectTerms(tree))
                .containsOnly(new QueryTerm("field", "d", QueryTerm.Type.EXACT));
        assertThat(analyzer.advancePhase(tree, advancer))
                .isFalse();

    }

    @Test
    public void testNestedDisjunctions() throws Exception {

        TreeAdvancer advancer = new TreeAdvancer.MinWeightTreeAdvancer(analyzer.weightor, 0);

        Query q = ParserUtils.parse("+(+((+aaaa +cc) (+dd +bbb +f)))");
        QueryTree tree = analyzer.buildTree(q);

        assertThat(analyzer.collectTerms(tree))
                .containsOnly(new QueryTerm("field", "aaaa", QueryTerm.Type.EXACT),
                              new QueryTerm("field", "bbb", QueryTerm.Type.EXACT));

        assertThat(analyzer.advancePhase(tree, advancer)).isTrue();
        assertThat(analyzer.collectTerms(tree))
                .containsOnly(new QueryTerm("field", "cc", QueryTerm.Type.EXACT),
                              new QueryTerm("field", "dd", QueryTerm.Type.EXACT));

        assertThat(analyzer.advancePhase(tree, advancer)).isTrue();
        assertThat(analyzer.collectTerms(tree))
                .containsOnly(new QueryTerm("field", "cc", QueryTerm.Type.EXACT),
                        new QueryTerm("field", "f", QueryTerm.Type.EXACT));

        assertThat(analyzer.advancePhase(tree, advancer)).isFalse();
    }

}
