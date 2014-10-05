package uk.co.flax.luwak.termextractor;

import org.apache.lucene.search.Query;
import org.junit.Test;
import uk.co.flax.luwak.termextractor.querytree.QueryTree;
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

    public static QueryAnalyzer analyzer = new QueryAnalyzer();

    @Test
    public void testAdvancesCollectDifferentTerms() throws Exception {

        Query q = ParserUtils.parse("field:(+hello +goodbye)");
        QueryTree querytree = analyzer.buildTree(q);

        assertThat(analyzer.collectTerms(querytree))
                .containsExactly(new QueryTerm("field", "goodbye", QueryTerm.Type.EXACT));

        assertThat(analyzer.advancePhase(querytree)).isTrue();

        assertThat(analyzer.collectTerms(querytree))
                .containsExactly(new QueryTerm("field", "hello", QueryTerm.Type.EXACT));

        assertThat(analyzer.advancePhase(querytree)).isFalse();

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
        assertThat(analyzer.advancePhase(tree)).isTrue();
        assertThat(analyzer.collectTerms(tree))
                .containsOnly(new QueryTerm("field", "hello", QueryTerm.Type.EXACT));
        assertThat(analyzer.advancePhase(tree)).isFalse();
        assertThat(analyzer.collectTerms(tree))
                .containsOnly(new QueryTerm("field", "hello", QueryTerm.Type.EXACT));

    }

}
