package uk.co.flax.luwak.termextractor;

import com.google.common.collect.ImmutableList;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.search.*;
import org.junit.Test;
import uk.co.flax.luwak.termextractor.querytree.TreeWeightor;
import uk.co.flax.luwak.termextractor.treebuilder.RegexpNGramTermQueryTreeBuilder;

import static org.assertj.core.api.Assertions.assertThat;

/*
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
public class TestExtractors {

    private static final QueryAnalyzer treeBuilder = new QueryAnalyzer();

    private static QueryAnalyzer getBuilder(QueryTreeBuilder<?>... queryTreeBuilder) {
        return new QueryAnalyzer(TreeWeightor.DEFAULT_WEIGHTOR, queryTreeBuilder);
    }

    @Test
    public void testRegexpExtractor() {

        RegexpNGramTermQueryTreeBuilder extractor = new RegexpNGramTermQueryTreeBuilder("XX", "WILDCARD");
        QueryAnalyzer builder = getBuilder(extractor);

        assertThat(builder.collectTerms(new RegexpQuery(new Term("field", "super.*califragilistic"))))
                .containsExactly(new QueryTerm("field", "califragilisticXX", QueryTerm.Type.CUSTOM, "WILDCARD"));

        assertThat(builder.collectTerms(new RegexpQuery(new Term("field", "hell."))))
                .containsExactly(new QueryTerm("field", "hellXX", QueryTerm.Type.CUSTOM, "WILDCARD"));

        assertThat(builder.collectTerms(new RegexpQuery(new Term("field", "hel?o"))))
                .containsExactly(new QueryTerm("field", "heXX", QueryTerm.Type.CUSTOM, "WILDCARD"));

    }

    @Test
    @SuppressWarnings("deprecation")
    public void testRangeQueriesReturnAnyToken() {

        LegacyNumericRangeQuery<Long> nrq = LegacyNumericRangeQuery.newLongRange("field", 0l, 10l, true, true);

        assertThat(treeBuilder.collectTerms(nrq))
                .hasSize(1)
                .extracting("type")
                .containsExactly(QueryTerm.Type.ANY);

        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        bq.add(nrq, BooleanClause.Occur.MUST);
        bq.add(new TermQuery(new Term("field", "term")), BooleanClause.Occur.MUST);

        assertThat(treeBuilder.collectTerms(bq.build()))
                .containsExactly(new QueryTerm("field", "term", QueryTerm.Type.EXACT));
    }

    @Test
    public void testConstantScoreQueryExtractor() {

        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        bq.add(new TermQuery(new Term("f", "q1")), BooleanClause.Occur.MUST);
        bq.add(new TermQuery(new Term("f", "q2")), BooleanClause.Occur.SHOULD);

        Query csqWithQuery = new ConstantScoreQuery(bq.build());
        assertThat(treeBuilder.collectTerms(csqWithQuery))
                .containsExactly(new QueryTerm("f", "q1", QueryTerm.Type.EXACT));
        
    }

    @Test
    public void testPhraseQueryExtractor() {

        PhraseQuery.Builder pq = new PhraseQuery.Builder();
        pq.add(new Term("f", "hello"));
        pq.add(new Term("f", "encyclopedia"));

        assertThat(treeBuilder.collectTerms(pq.build()))
                .containsOnly(new QueryTerm("f", "encyclopedia", QueryTerm.Type.EXACT));

    }

    @Test
    public void testBoostQueryExtractor() {

        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        bq.add(new TermQuery(new Term("f", "q1")), BooleanClause.Occur.MUST);
        bq.add(new TermQuery(new Term("f", "q2")), BooleanClause.Occur.SHOULD);

        Query boostQuery = new BoostQuery(bq.build(), 0.5f);
        assertThat(treeBuilder.collectTerms(boostQuery))
                .containsExactly(new QueryTerm("f", "q1", QueryTerm.Type.EXACT));
    }

    @Test
    public void testDisjunctionMaxExtractor() {

        Query query = new DisjunctionMaxQuery(
                ImmutableList.<Query>of(new TermQuery(new Term("f", "t1")), new TermQuery(new Term("f", "t2"))), 0.1f
        );
        assertThat(treeBuilder.collectTerms(query))
                .hasSize(2)
                .containsExactly(new QueryTerm("f", "t1", QueryTerm.Type.EXACT), new QueryTerm("f", "t2", QueryTerm.Type.EXACT));
    }

    @Test
    public void testTermsQueryExtractor() {
        Query q = new TermsQuery(new Term("f1", "t1"), new Term("f2", "t2"));
        assertThat(treeBuilder.collectTerms(q))
                .containsExactly(new QueryTerm("f1", "t1", QueryTerm.Type.EXACT), new QueryTerm("f2", "t2", QueryTerm.Type.EXACT));
    }

    @Test
    public void testBooleanExtractsFilter() {
        Query q = new BooleanQuery.Builder()
                .add(new TermQuery(new Term("f", "must")), BooleanClause.Occur.MUST)
                .add(new TermQuery(new Term("f", "filter")), BooleanClause.Occur.FILTER)
                .build();
        assertThat(treeBuilder.collectTerms(q))
                .containsExactly(new QueryTerm("f", "filter", QueryTerm.Type.EXACT)); // it's longer, so it wins
    }


}
