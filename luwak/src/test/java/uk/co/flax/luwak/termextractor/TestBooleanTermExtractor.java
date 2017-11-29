package uk.co.flax.luwak.termextractor;

import java.util.List;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.assertj.core.api.Condition;
import org.junit.Test;
import uk.co.flax.luwak.termextractor.querytree.AnyNode;
import uk.co.flax.luwak.testutils.ParserUtils;

import static org.assertj.core.api.Assertions.assertThat;

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

public class TestBooleanTermExtractor {

    private static final QueryAnalyzer treeBuilder = new QueryAnalyzer();

    @Test
    public void allDisjunctionQueriesAreIncluded() throws Exception {

        Query bq = ParserUtils.parse("field1:term1 field1:term2");
        List<QueryTerm> terms = treeBuilder.collectTerms(bq);

        assertThat(terms).containsOnly(
                new QueryTerm("field1", "term1", QueryTerm.Type.EXACT),
                new QueryTerm("field1", "term2", QueryTerm.Type.EXACT));

    }

    @Test
    public void allNestedDisjunctionClausesAreIncluded() throws Exception {

        Query q = ParserUtils.parse("field1:term3 (field1:term1 field1:term2)");

        assertThat(treeBuilder.collectTerms(q)).hasSize(3);
    }

    @Test
    public void allDisjunctionClausesOfAConjunctionAreExtracted() throws Exception {

        Query q = ParserUtils.parse("+(field1:term1 field1:term2) field1:term3");

        assertThat(treeBuilder.collectTerms(q)).hasSize(2);

    }

    @Test
    public void conjunctionsOutweighDisjunctions() throws Exception {
        Query bq = ParserUtils.parse("field1:term1 +field1:term2");

        assertThat(treeBuilder.collectTerms(bq))
                .containsOnly(new QueryTerm("field1", "term2", QueryTerm.Type.EXACT));
    }

    @Test
    public void disjunctionsWithPureNegativeClausesReturnANYTOKEN() throws Exception {

        Query q = ParserUtils.parse("+field1:term1 +(field2:term22 (-field2:notterm))");

        assertThat(treeBuilder.collectTerms(q))
                .containsOnly(new QueryTerm("field1", "term1", QueryTerm.Type.EXACT));

    }

    @Test
    public void disjunctionsWithMatchAllNegativeClausesReturnANYTOKEN() throws Exception {

        Query q = ParserUtils.parse("+field1:term1 +(field2:term22 (*:* -field2:notterm))");

        assertThat(treeBuilder.collectTerms(q))
                .containsOnly(new QueryTerm("field1", "term1", QueryTerm.Type.EXACT));

    }

    @Test
    public void testMatchAllDocsIsOnlyQuery() throws Exception {
        // Set up - single MatchAllDocsQuery clause in a BooleanQuery
        Query q = ParserUtils.parse("+*:*");
        assertThat(q).isInstanceOf(BooleanQuery.class);
        BooleanClause clause = Iterables.getOnlyElement((BooleanQuery)q);
        assertThat(clause.getQuery()).isInstanceOf(MatchAllDocsQuery.class);
        assertThat(clause.getOccur()).isSameAs(BooleanClause.Occur.MUST);

        List<QueryTerm> terms = treeBuilder.collectTerms(q);
        assertThat(terms).hasSize(1);
        assertThat(terms.get(0).type).isSameAs(QueryTerm.Type.ANY);
    }

    @Test
    public void testMatchAllDocsMustWithKeywordShould() throws Exception {
        Query q = ParserUtils.parse("+*:* field1:term1");
        // Because field1:term1 is optional, only the MatchAllDocsQuery is collected.
        List<QueryTerm> terms = treeBuilder.collectTerms(q);
        assertThat(terms).hasSize(1);
        assertThat(terms.get(0).type).isSameAs(QueryTerm.Type.ANY);
    }

    @Test
    public void testMatchAllDocsMustWithKeywordNot() throws Exception {
        Query q = ParserUtils.parse("+*:* -field1:notterm");

        // Because field1:notterm is negated, only the mandatory MatchAllDocsQuery is collected.
        List<QueryTerm> terms = treeBuilder.collectTerms(q);
        assertThat(terms).hasSize(1);
        assertThat(terms.get(0).type).isSameAs(QueryTerm.Type.ANY);
    }

    @Test
    public void testMatchAllDocsMustWithKeywordShouldAndKeywordNot() throws Exception {
        Query q = ParserUtils.parse("+*:* field1:term1 -field2:notterm");

        // Because field1:notterm is negated and field1:term1 is optional, only the mandatory MatchAllDocsQuery is collected.
        List<QueryTerm> terms = treeBuilder.collectTerms(q);
        assertThat(terms).hasSize(1);
        assertThat(terms.get(0).type).isSameAs(QueryTerm.Type.ANY);
    }

    @Test
    public void testMatchAllDocsMustAndOtherMustWithKeywordShouldAndKeywordNot() throws Exception {
        Query q = ParserUtils.parse("+*:* +field9:term9 field1:term1 -field2:notterm");

        // The queryterm collected by weight is the non-anynode, so field9:term9 shows up before MatchAllDocsQuery.
        List<QueryTerm> terms = treeBuilder.collectTerms(q);
        assertThat(terms).hasSize(1);
        assertThat(terms.get(0).type).isSameAs(QueryTerm.Type.EXACT);
        assertThat(terms.get(0).term).isEqualTo(new Term("field9", "term9"));
    }

}
