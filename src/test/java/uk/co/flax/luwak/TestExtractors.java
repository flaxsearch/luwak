package uk.co.flax.luwak;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.search.intervals.FieldedBooleanQuery;
import org.apache.lucene.search.intervals.OrderedNearQuery;
import org.apache.lucene.search.intervals.UnorderedNearQuery;
import org.junit.Test;
import uk.co.flax.luwak.termextractor.QueryTerm;
import uk.co.flax.luwak.termextractor.QueryTermExtractor;
import uk.co.flax.luwak.termextractor.RegexpNGramTermExtractor;

import java.util.ArrayList;
import java.util.List;
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
public class TestExtractors {

    @Test
    public void testRegexpExtractor() {

        RegexpNGramTermExtractor extractor = new RegexpNGramTermExtractor();
        List<QueryTerm> terms = new ArrayList<>();
        RegexpQuery query = new RegexpQuery(new Term("field", "super.*califragilistic"));

        extractor.extract(query, terms, null);

        assertThat(terms).containsExactly(new QueryTerm("field", "califragilistic", QueryTerm.Type.WILDCARD));

    }

    @Test
    public void testOrderedNearExtractor() {
        QueryTermExtractor qte = new QueryTermExtractor();

        OrderedNearQuery q = new OrderedNearQuery(0,
                new TermQuery(new Term("field1", "term1")),
                new TermQuery(new Term("field1", "term2")));

        Set<QueryTerm> terms = qte.extract(q);

        assertThat(terms).containsExactly(new QueryTerm("field1", "term1", QueryTerm.Type.EXACT));
    }

    @Test
    public void testUnorderedNearExtractor() {
        QueryTermExtractor qte = new QueryTermExtractor();
        UnorderedNearQuery q = new UnorderedNearQuery(0,
                new TermQuery(new Term("field1", "term1")),
                new TermQuery(new Term("field1", "term2")));

        Set<QueryTerm> terms = qte.extract(q);

        assertThat(terms).containsExactly(new QueryTerm("field1", "term1", QueryTerm.Type.EXACT));
    }

    @Test
    public void testOrderedNearWithWildcardExtractor() {
        QueryTermExtractor qte = new QueryTermExtractor();
        OrderedNearQuery q = new OrderedNearQuery(0,
                new RegexpQuery(new Term("field", "super.*cali.*")),
                new TermQuery(new Term("field", "is")));
        Set<QueryTerm> terms = qte.extract(q);

        assertThat(terms).containsExactly(new QueryTerm("field", "is", QueryTerm.Type.EXACT));
    }

    @Test
    public void testRangeQueriesReturnAnyToken() {
        QueryTermExtractor qte = new QueryTermExtractor();
        NumericRangeQuery<Long> nrq = NumericRangeQuery.newLongRange("field", 0l, 10l, true, true);
        Set<QueryTerm> terms = qte.extract(nrq);

        assertThat(terms).containsExactly(new QueryTerm("field", "field:[0 TO 10]", QueryTerm.Type.ANY));

        BooleanQuery bq = new BooleanQuery();
        bq.add(nrq, BooleanClause.Occur.MUST);
        bq.add(new TermQuery(new Term("field", "term")), BooleanClause.Occur.MUST);

        terms = qte.extract(bq);
        assertThat(terms).containsExactly(new QueryTerm("field", "term", QueryTerm.Type.EXACT));
    }

    @Test
    public void testFieldedBooleanQuery() {
        QueryTermExtractor qte = new QueryTermExtractor();
        BooleanQuery bq = new BooleanQuery();
        bq.add(new TermQuery(new Term("field1", "term1")), BooleanClause.Occur.MUST);
        bq.add(new TermQuery(new Term("field1", "term2")), BooleanClause.Occur.MUST);
        FieldedBooleanQuery q = new FieldedBooleanQuery(bq);

        Set<QueryTerm> terms = qte.extract(q);

        assertThat(terms).containsExactly(new QueryTerm("field1", "term1", QueryTerm.Type.EXACT));
    }
}
