package uk.co.flax.luwak.termextractor;

import com.google.common.collect.ImmutableList;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;
import uk.co.flax.luwak.termextractor.treebuilder.RegexpNGramTermQueryTreeBuilder;
import uk.co.flax.luwak.termextractor.weights.TermWeightor;
import uk.co.flax.luwak.termextractor.weights.TokenLengthNorm;

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
        return new QueryAnalyzer(queryTreeBuilder);
    }

    private static final TermWeightor WEIGHTOR = new TermWeightor(new TokenLengthNorm());

    @Test
    public void testRegexpExtractor() {

        RegexpNGramTermQueryTreeBuilder extractor = new RegexpNGramTermQueryTreeBuilder("XX", "WILDCARD");
        QueryAnalyzer builder = getBuilder(extractor);

        assertThat(builder.collectTerms(new RegexpQuery(new Term("field", "super.*califragilistic")), WEIGHTOR))
                .containsExactly(new QueryTerm("field", "califragilisticXX", QueryTerm.Type.CUSTOM, "WILDCARD"));

        assertThat(builder.collectTerms(new RegexpQuery(new Term("field", "hell.")), WEIGHTOR))
                .containsExactly(new QueryTerm("field", "hellXX", QueryTerm.Type.CUSTOM, "WILDCARD"));

        assertThat(builder.collectTerms(new RegexpQuery(new Term("field", "hel?o")), WEIGHTOR))
                .containsExactly(new QueryTerm("field", "heXX", QueryTerm.Type.CUSTOM, "WILDCARD"));

    }

    @Test
    public void testConstantScoreQueryExtractor() {

        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        bq.add(new TermQuery(new Term("f", "q1")), BooleanClause.Occur.MUST);
        bq.add(new TermQuery(new Term("f", "q2")), BooleanClause.Occur.SHOULD);

        Query csqWithQuery = new ConstantScoreQuery(bq.build());
        assertThat(treeBuilder.collectTerms(csqWithQuery, WEIGHTOR))
                .containsExactly(new QueryTerm("f", "q1", QueryTerm.Type.EXACT));
        
    }

    @Test
    public void testPhraseQueryExtractor() {

        PhraseQuery.Builder pq = new PhraseQuery.Builder();
        pq.add(new Term("f", "hello"));
        pq.add(new Term("f", "encyclopedia"));

        assertThat(treeBuilder.collectTerms(pq.build(), WEIGHTOR))
                .containsOnly(new QueryTerm("f", "encyclopedia", QueryTerm.Type.EXACT));

    }

    @Test
    public void testBoostQueryExtractor() {

        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        bq.add(new TermQuery(new Term("f", "q1")), BooleanClause.Occur.MUST);
        bq.add(new TermQuery(new Term("f", "q2")), BooleanClause.Occur.SHOULD);

        Query boostQuery = new BoostQuery(bq.build(), 0.5f);
        assertThat(treeBuilder.collectTerms(boostQuery, WEIGHTOR))
                .containsExactly(new QueryTerm("f", "q1", QueryTerm.Type.EXACT));
    }

    @Test
    public void testDisjunctionMaxExtractor() {

        Query query = new DisjunctionMaxQuery(
                ImmutableList.<Query>of(new TermQuery(new Term("f", "t1")), new TermQuery(new Term("f", "t2"))), 0.1f
        );
        assertThat(treeBuilder.collectTerms(query, WEIGHTOR))
                .hasSize(2)
                .containsOnly(new QueryTerm("f", "t1", QueryTerm.Type.EXACT), new QueryTerm("f", "t2", QueryTerm.Type.EXACT));
    }

    @Test
    public void testTermInSetQueryExtractor() {
        Query q = new TermInSetQuery("f1", new BytesRef("t1"), new BytesRef("t2"));
        assertThat(treeBuilder.collectTerms(q, WEIGHTOR))
                .containsOnly(new QueryTerm("f1", "t1", QueryTerm.Type.EXACT), new QueryTerm("f1", "t2", QueryTerm.Type.EXACT));
    }

    @Test
    public void testBooleanExtractsFilter() {
        Query q = new BooleanQuery.Builder()
                .add(new TermQuery(new Term("f", "must")), BooleanClause.Occur.MUST)
                .add(new TermQuery(new Term("f", "filter")), BooleanClause.Occur.FILTER)
                .build();
        assertThat(treeBuilder.collectTerms(q, WEIGHTOR))
                .containsExactly(new QueryTerm("f", "filter", QueryTerm.Type.EXACT)); // it's longer, so it wins
    }


}
