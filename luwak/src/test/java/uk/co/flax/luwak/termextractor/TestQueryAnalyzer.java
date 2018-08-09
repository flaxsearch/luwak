package uk.co.flax.luwak.termextractor;

import org.apache.lucene.search.Query;
import org.junit.Test;
import uk.co.flax.luwak.termextractor.querytree.QueryTree;
import uk.co.flax.luwak.termextractor.querytree.QueryTreeViewer;
import uk.co.flax.luwak.termextractor.weights.TermWeightNorm;
import uk.co.flax.luwak.termextractor.weights.TermWeightor;
import uk.co.flax.luwak.termextractor.weights.TokenLengthNorm;
import uk.co.flax.luwak.testutils.ParserUtils;

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

public class TestQueryAnalyzer {

    public static final QueryAnalyzer analyzer = new QueryAnalyzer();

    @Test
    public void testAdvancesCollectDifferentTerms() throws Exception {

        Query q = ParserUtils.parse("field:(+hello +goodbye)");
        QueryTree querytree = analyzer.buildTree(q, TermWeightor.DEFAULT);

        assertThat(analyzer.collectTerms(querytree))
                .containsExactly(new QueryTerm("field", "goodbye", QueryTerm.Type.EXACT));

        assertThat(querytree.advancePhase()).isTrue();

        assertThat(analyzer.collectTerms(querytree))
                .containsExactly(new QueryTerm("field", "hello", QueryTerm.Type.EXACT));

        assertThat(querytree.advancePhase()).isFalse();

        assertThat(analyzer.collectTerms(querytree))
                .containsExactly(new QueryTerm("field", "hello", QueryTerm.Type.EXACT));

    }

    @Test
    public void testDisjunctionsWithAnyClausesOnlyReturnANYTOKEN() throws Exception {

        // disjunction containing a pure negative - we can't narrow this down
        Query q = ParserUtils.parse("hello goodbye (*:* -term)");

        assertThat(analyzer.collectTerms(q, TermWeightor.DEFAULT))
                .extracting("type")
                .containsOnly(QueryTerm.Type.ANY);

    }

    @Test
    public void testConjunctionsCannotAdvanceOverANYTOKENs() throws Exception {

        Query q = ParserUtils.parse("+hello +howdyedo +(goodbye (*:* -whatever))");
        QueryTree tree = analyzer.buildTree(q, TermWeightor.DEFAULT);

        assertThat(analyzer.collectTerms(tree))
                .containsOnly(new QueryTerm("field", "howdyedo", QueryTerm.Type.EXACT));
        assertThat(tree.advancePhase()).isTrue();
        assertThat(analyzer.collectTerms(tree))
                .containsOnly(new QueryTerm("field", "hello", QueryTerm.Type.EXACT));
        QueryTreeViewer.view(tree, System.out);
        assertThat(tree.advancePhase()).isFalse();
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
        assertThat(tree.advancePhase())
                .isTrue();
        assertThat(analyzer.collectTerms(tree))
                .containsOnly(new QueryTerm("field", "hello", QueryTerm.Type.EXACT));
        QueryTreeViewer.view(tree, System.out);
        assertThat(tree.advancePhase())
                .isFalse();
        QueryTreeViewer.view(tree, System.out);

    }

    @Test
    public void testNestedConjunctions() throws Exception {

        Query q = ParserUtils.parse("+(+(+(+aaaa +cc) +(+d +bbb)))");
        QueryTree tree = analyzer.buildTree(q, TermWeightor.DEFAULT);

        assertThat(analyzer.collectTerms(tree))
                .containsOnly(new QueryTerm("field", "aaaa", QueryTerm.Type.EXACT));
        assertThat(tree.advancePhase())
                .isTrue();

        assertThat(analyzer.collectTerms(tree))
                .containsOnly(new QueryTerm("field", "bbb", QueryTerm.Type.EXACT));
        assertThat(tree.advancePhase())
                .isTrue();

        assertThat(analyzer.collectTerms(tree))
                .containsOnly(new QueryTerm("field", "cc", QueryTerm.Type.EXACT));
        assertThat(tree.advancePhase())
                .isTrue();

        assertThat(analyzer.collectTerms(tree))
                .containsOnly(new QueryTerm("field", "d", QueryTerm.Type.EXACT));
        assertThat(tree.advancePhase())
                .isFalse();

    }

    @Test
    public void testNestedDisjunctions() throws Exception {

        Query q = ParserUtils.parse("+(+((+aaaa +cc) (+dd +bbb +f)))");
        QueryTree tree = analyzer.buildTree(q, TermWeightor.DEFAULT);

        assertThat(analyzer.collectTerms(tree))
                .containsOnly(new QueryTerm("field", "aaaa", QueryTerm.Type.EXACT),
                              new QueryTerm("field", "bbb", QueryTerm.Type.EXACT));

        assertThat(tree.advancePhase()).isTrue();
        assertThat(analyzer.collectTerms(tree))
                .containsOnly(new QueryTerm("field", "cc", QueryTerm.Type.EXACT),
                              new QueryTerm("field", "dd", QueryTerm.Type.EXACT));

        assertThat(tree.advancePhase()).isTrue();
        assertThat(analyzer.collectTerms(tree))
                .containsOnly(new QueryTerm("field", "cc", QueryTerm.Type.EXACT),
                        new QueryTerm("field", "f", QueryTerm.Type.EXACT));

        assertThat(tree.advancePhase()).isFalse();
    }

}
