package uk.co.flax.luwak.termextractor;

import java.util.List;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TermQuery;
import org.junit.Test;
import uk.co.flax.luwak.presearcher.PresearcherComponent;

import static org.fest.assertions.api.Assertions.assertThat;
import static uk.co.flax.luwak.termextractor.BooleanQueryUtils.BQBuilder;
import static uk.co.flax.luwak.termextractor.BooleanQueryUtils.newTermQuery;

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

    private static final QueryAnalyzer treeBuilder = PresearcherComponent.buildQueryAnalyzer();

    @Test
    public void allDisjunctionQueriesAreIncluded() {

        BooleanQuery bq = new BooleanQuery();
        bq.add(new TermQuery(new Term("field1", "term1")), BooleanClause.Occur.SHOULD);
        bq.add(new TermQuery(new Term("field1", "term2")), BooleanClause.Occur.SHOULD);

        List<QueryTerm> terms = treeBuilder.collectTerms(bq);

        assertThat(terms).containsOnly(
                new QueryTerm("field1", "term1", QueryTerm.Type.EXACT),
                new QueryTerm("field1", "term2", QueryTerm.Type.EXACT));

    }

    @Test
    public void allNestedDisjunctionClausesAreIncluded() {
        BooleanQuery superbq = BQBuilder.newBQ()
                .addShouldClause(newTermQuery("field1", "term3"))
                .addShouldClause(BooleanQueryUtils.BQBuilder.newBQ()
                        .addShouldClause(newTermQuery("field1", "term1"))
                        .addShouldClause(newTermQuery("field1", "term2"))
                        .build())
                .build();

        assertThat(treeBuilder.collectTerms(superbq)).hasSize(3);
    }

    @Test
    public void allDisjunctionClausesOfAConjunctionAreExtracted() {

        BooleanQuery superbq = BooleanQueryUtils.BQBuilder.newBQ()
                .addMustClause(BooleanQueryUtils.BQBuilder.newBQ()
                        .addShouldClause(newTermQuery("field1", "term1"))
                        .addShouldClause(newTermQuery("field1", "term2"))
                        .build())
                .addShouldClause(newTermQuery("field1", "term3"))
                .build();

        assertThat(treeBuilder.collectTerms(superbq)).hasSize(2);

    }

    @Test
    public void conjunctionsOutweighDisjunctions() {
        BooleanQuery bq = new BooleanQuery();
        bq.add(new TermQuery(new Term("field1", "term1")), BooleanClause.Occur.SHOULD);
        bq.add(new TermQuery(new Term("field1", "term2")), BooleanClause.Occur.MUST);

        assertThat(treeBuilder.collectTerms(bq))
                .containsOnly(new QueryTerm("field1", "term2", QueryTerm.Type.EXACT));
    }

    @Test
    public void disjunctionsWithPureNegativeClausesReturnANYTOKEN() {

        BooleanQuery q = BQBuilder.newBQ()
                .addMustClause(new TermQuery(new Term("field1", "term1")))
                .addMustClause(BQBuilder.newBQ()
                        .addShouldClause(new TermQuery(new Term("field2", "term22")))
                        .addShouldClause(BQBuilder.newBQ()
                                .addNotClause(new TermQuery(new Term("field2", "notterm")))
                                .build())
                        .build())
                .build();

        assertThat(treeBuilder.collectTerms(q))
                .containsOnly(new QueryTerm("field1", "term1", QueryTerm.Type.EXACT));

    }

    @Test
    public void disjunctionsWithMatchAllNegativeClausesReturnANYTOKEN() {

        BooleanQuery q = BQBuilder.newBQ()
                .addMustClause(new TermQuery(new Term("field1", "term1")))
                .addMustClause(BQBuilder.newBQ()
                        .addShouldClause(new TermQuery(new Term("field2", "term22")))
                        .addShouldClause(BQBuilder.newBQ()
                                .addShouldClause(new MatchAllDocsQuery())
                                .addNotClause(new TermQuery(new Term("field2", "notterm")))
                                .build())
                        .build())
                .build();

        assertThat(treeBuilder.collectTerms(q))
                .containsOnly(new QueryTerm("field1", "term1", QueryTerm.Type.EXACT));

    }

}
