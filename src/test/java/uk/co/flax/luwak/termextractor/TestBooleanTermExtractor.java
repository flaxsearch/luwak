package uk.co.flax.luwak.termextractor;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.junit.Test;
import uk.co.flax.luwak.termextractor.QueryTerm;
import uk.co.flax.luwak.termextractor.QueryTermExtractor;

import java.util.Set;

import static org.fest.assertions.api.Assertions.assertThat;

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
    public void bestConjunctionQueriesAreIncluded() {
        BooleanQuery bq = new BooleanQuery();
        bq.add(new TermQuery(new Term("field1", "term1")), BooleanClause.Occur.SHOULD);
        bq.add(new TermQuery(new Term("field1", "term2")), BooleanClause.Occur.MUST);

        QueryTermExtractor qte = new QueryTermExtractor();
        Set<QueryTerm> terms = qte.extract(bq);

        assertThat(terms).containsOnly(new QueryTerm("field1", "term2", QueryTerm.Type.EXACT));
    }

    @Test
    public void allNestedDisjunctionClausesAreIncluded() {
        BooleanQuery superbq = BQBuilder.newBQ()
                .addShouldClause(newTermQuery("field1", "term3"))
                .addShouldClause(BQBuilder.newBQ()
                        .addShouldClause(newTermQuery("field1", "term1"))
                        .addShouldClause(newTermQuery("field1", "term2"))
                        .build())
                .build();

        assertThat(extract(superbq)).hasSize(3);
    }

    @Test
    public void allDisjunctionClausesOfAConjunctionAreExtracted() {

        BooleanQuery superbq = BQBuilder.newBQ()
                .addMustClause(BQBuilder.newBQ()
                        .addShouldClause(newTermQuery("field1", "term1"))
                        .addShouldClause(newTermQuery("field1", "term2"))
                        .build())
                .addShouldClause(newTermQuery("field1", "term3"))
                .build();

        assertThat(extract(superbq)).hasSize(2);

    }

    @Test
    public void exactClausesPreferred() {
        BooleanQuery bq = BQBuilder.newBQ()
                .addMustClause(new RegexpQuery(new Term("field1", "term?")))
                .addMustClause(BQBuilder.newBQ()
                        .addShouldClause(newTermQuery("field1", "term1"))
                        .addShouldClause(newTermQuery("field1", "term2"))
                        .build()
                )
                .build();

        assertThat(extract(bq))
                .hasSize(2);
    }

    public static Set<QueryTerm> extract(Query query) {
        return new QueryTermExtractor().extract(query);
    }

    public static TermQuery newTermQuery(String field, String text) {
        return new TermQuery(new Term(field, text));
    }

    public static class BQBuilder {

        private final BooleanQuery bq;

        static BQBuilder newBQ() {
            return new BQBuilder();
        }

        BQBuilder() {
            this.bq = new BooleanQuery();
        }

        BooleanQuery build() {
            return bq;
        }

        public BQBuilder addMustClause(Query subQuery) {
            bq.add(subQuery, BooleanClause.Occur.MUST);
            return this;
        }

        public BQBuilder addShouldClause(Query subQuery) {
            bq.add(subQuery, BooleanClause.Occur.SHOULD);
            return this;
        }
    }

}
