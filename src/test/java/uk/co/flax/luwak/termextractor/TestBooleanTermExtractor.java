package uk.co.flax.luwak.termextractor;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.junit.Test;

import java.util.Set;

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

    @Test
    public void allDisjunctionQueriesAreIncluded() {

        BooleanQuery bq = new BooleanQuery();
        bq.add(new TermQuery(new Term("field1", "term1")), BooleanClause.Occur.SHOULD);
        bq.add(new TermQuery(new Term("field1", "term2")), BooleanClause.Occur.SHOULD);

        QueryTermExtractor qte = new QueryTermExtractor();
        Set<QueryTerm> terms = qte.extract(bq);

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

        assertThat(extract(superbq)).hasSize(3);
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

        assertThat(extract(superbq)).hasSize(2);

    }

    @Test
    public void conjunctionsOutweighDisjunctions() {
        BooleanQuery bq = new BooleanQuery();
        bq.add(new TermQuery(new Term("field1", "term1")), BooleanClause.Occur.SHOULD);
        bq.add(new TermQuery(new Term("field1", "term2")), BooleanClause.Occur.MUST);

        QueryTermExtractor qte = new QueryTermExtractor();
        Set<QueryTerm> terms = qte.extract(bq);

        assertThat(terms).containsOnly(new QueryTerm("field1", "term2", QueryTerm.Type.EXACT));
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

        QueryTermExtractor qte = new QueryTermExtractor();
        Set<QueryTerm> terms = qte.extract(q);

        assertThat(terms).containsOnly(new QueryTerm("field1", "term1", QueryTerm.Type.EXACT));

    }

    public static Set<QueryTerm> extract(Query query) {
        return new QueryTermExtractor().extract(query);
    }

}
