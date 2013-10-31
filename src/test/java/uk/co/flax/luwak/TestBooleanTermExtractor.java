package uk.co.flax.luwak;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.TermQuery;
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
        BooleanQuery bq = new BooleanQuery();
        bq.add(new TermQuery(new Term("field1", "term1")), BooleanClause.Occur.SHOULD);
        bq.add(new TermQuery(new Term("field1", "term2")), BooleanClause.Occur.SHOULD);
        BooleanQuery superbq = new BooleanQuery();
        superbq.add(new TermQuery(new Term("field1", "term3")), BooleanClause.Occur.SHOULD);
        superbq.add(bq, BooleanClause.Occur.SHOULD);

        QueryTermExtractor qte = new QueryTermExtractor();
        Set<QueryTerm> terms = qte.extract(superbq);

        assertThat(terms).hasSize(3);
    }

}
