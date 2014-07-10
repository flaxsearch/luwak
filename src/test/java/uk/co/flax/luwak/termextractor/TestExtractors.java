package uk.co.flax.luwak.termextractor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermFilter;
import org.apache.lucene.queries.TermsFilter;
import org.apache.lucene.search.*;
import org.apache.lucene.search.intervals.FieldedBooleanQuery;
import org.apache.lucene.search.intervals.OrderedNearQuery;
import org.apache.lucene.search.intervals.UnorderedNearQuery;
import org.apache.lucene.util.Bits;
import org.junit.Test;

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

        RegexpNGramTermExtractor extractor = new RegexpNGramTermExtractor("XX");
        RegexpQuery query = new RegexpQuery(new Term("field", "super.*califragilistic"));

        List<QueryTerm> terms = new ArrayList<>();
        extractor.extract(query, terms, null);

        assertThat(terms).containsExactly(new QueryTerm("field", "califragilisticXX", QueryTerm.Type.WILDCARD));

        terms.clear();
        extractor.extract(new RegexpQuery(new Term("field", "hell.")), terms, null);
        assertThat(terms).containsExactly(new QueryTerm("field", "hellXX", QueryTerm.Type.WILDCARD));

        terms.clear();
        extractor.extract(new RegexpQuery(new Term("field", "hel?o")), terms, null);
        assertThat(terms).containsExactly(new QueryTerm("field", "heXX", QueryTerm.Type.WILDCARD));

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

    @Test
    public void testFilteredQueryTermExtractor() {

        QueryTermExtractor qte = new QueryTermExtractor(new FilteredQueryExtractor());

        Query q = new TermQuery(new Term("field", "term"));
        Filter f = new TermFilter(new Term("field", "filterterm"));
        FilteredQuery fq = new FilteredQuery(q, f);

        Set<QueryTerm> terms = qte.extract(fq);
        assertThat(terms).hasSize(1);   // treat filterquery as a conjunction, only need one subclause

        // selects 'filterterm' over 'term' because it's longer
        assertThat(terms).containsExactly(new QueryTerm("field", "filterterm", QueryTerm.Type.EXACT));

    }

    private static class MyFilter extends Filter {

        @Override
        public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
            return null;
        }
    }

    private static class MyFilterTermExtractor extends Extractor<MyFilter> {

        protected MyFilterTermExtractor() {
            super(MyFilter.class);
        }

        @Override
        public void extract(MyFilter filter, List<QueryTerm> terms, List<Extractor<?>> extractors) {
            terms.add(new QueryTerm("FILTER", "MYFILTER", QueryTerm.Type.EXACT));
        }
    }

    @Test
    public void testExtendedFilteredQueryExtractor() {

        QueryTermExtractor qte = new QueryTermExtractor(new MyFilterTermExtractor());

        Query q = new RegexpQuery(new Term("FILTER", "*"));
        Filter f = new MyFilter();

        Set<QueryTerm> terms = qte.extract(new FilteredQuery(q, f));
        assertThat(terms).containsExactly(new QueryTerm("FILTER", "MYFILTER", QueryTerm.Type.EXACT));

    }

    @Test
    public void testConstantScoreQueryExtractor() {

        QueryTermExtractor qte = new QueryTermExtractor();

        BooleanQuery bq = new BooleanQuery();
        bq.add(new TermQuery(new Term("f", "q1")), BooleanClause.Occur.MUST);
        bq.add(new TermQuery(new Term("f", "q2")), BooleanClause.Occur.SHOULD);

        Query csqWithQuery = new ConstantScoreQuery(bq);
        assertThat(qte.extract(csqWithQuery))
                .containsExactly(new QueryTerm("f", "q1", QueryTerm.Type.EXACT));


        TermsFilter tf = new TermsFilter(new Term("f", "q1"), new Term("f", "q22"));

        Query csqWithFilter = new ConstantScoreQuery(tf);
        assertThat(qte.extract(csqWithFilter))
                .containsOnly(new QueryTerm("f", "q1", QueryTerm.Type.EXACT), new QueryTerm("f", "q22", QueryTerm.Type.EXACT));

    }

    @Test
    public void testPhraseQueryExtractor() {

        QueryTermExtractor qte = new QueryTermExtractor();

        PhraseQuery pq = new PhraseQuery();
        pq.add(new Term("f", "hello"));
        pq.add(new Term("f", "encyclopedia"));

        assertThat(qte.extract(pq)).containsOnly(new QueryTerm("f", "encyclopedia", QueryTerm.Type.EXACT));

    }

}
