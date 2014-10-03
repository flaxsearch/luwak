package uk.co.flax.luwak.termextractor;

import java.io.IOException;

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
import uk.co.flax.luwak.termextractor.treebuilder.RegexpNGramTermQueryTreeBuilder;
import uk.co.flax.luwak.termextractor.querytree.QueryTree;
import uk.co.flax.luwak.termextractor.querytree.TermNode;
import uk.co.flax.luwak.termextractor.querytree.TreeWeightor;

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

    private static final QueryAnalyzer treeBuilder = new QueryAnalyzer(TreeWeightor.DEFAULT_WEIGHTOR);

    private static QueryAnalyzer getBuilder(QueryTreeBuilder<?> queryTreeBuilder) {
        return new QueryAnalyzer(TreeWeightor.DEFAULT_WEIGHTOR, queryTreeBuilder);
    }

    @Test
    public void testRegexpExtractor() {

        RegexpNGramTermQueryTreeBuilder extractor = new RegexpNGramTermQueryTreeBuilder("XX", "WILDCARD");
        QueryAnalyzer builder = getBuilder(extractor);

        assertThat(builder.collectTerms(new RegexpQuery(new Term("field", "super.*califragilistic"))))
                .containsExactly(new QueryTerm("field", "califragilisticXX", QueryTerm.Type.CUSTOM("WILDCARD")));

        assertThat(builder.collectTerms(new RegexpQuery(new Term("field", "hell."))))
                .containsExactly(new QueryTerm("field", "hellXX", QueryTerm.Type.CUSTOM("WILDCARD")));

        assertThat(builder.collectTerms(new RegexpQuery(new Term("field", "hel?o"))))
                .containsExactly(new QueryTerm("field", "heXX", QueryTerm.Type.CUSTOM("WILDCARD")));

    }

    @Test
    public void testOrderedNearExtractor() {
        OrderedNearQuery q = new OrderedNearQuery(0,
                new TermQuery(new Term("field1", "term1")),
                new TermQuery(new Term("field1", "term")));

        assertThat(treeBuilder.collectTerms(q))
                .containsExactly(new QueryTerm("field1", "term1", QueryTerm.Type.EXACT));
    }

    @Test
    public void testUnorderedNearExtractor() {
        UnorderedNearQuery q = new UnorderedNearQuery(0,
                new TermQuery(new Term("field1", "term1")),
                new TermQuery(new Term("field1", "term")));

        assertThat(treeBuilder.collectTerms(q)).containsExactly(new QueryTerm("field1", "term1", QueryTerm.Type.EXACT));
    }

    @Test
    public void testOrderedNearWithWildcardExtractor() {
        OrderedNearQuery q = new OrderedNearQuery(0,
                new RegexpQuery(new Term("field", "super.*cali.*")),
                new TermQuery(new Term("field", "is")));

        assertThat(treeBuilder.collectTerms(q)).containsExactly(new QueryTerm("field", "is", QueryTerm.Type.EXACT));
    }

    @Test
    public void testRangeQueriesReturnAnyToken() {

        NumericRangeQuery<Long> nrq = NumericRangeQuery.newLongRange("field", 0l, 10l, true, true);

        assertThat(treeBuilder.collectTerms(nrq))
                .containsExactly(new QueryTerm("field", "field:[0 TO 10]", QueryTerm.Type.ANY));

        BooleanQuery bq = new BooleanQuery();
        bq.add(nrq, BooleanClause.Occur.MUST);
        bq.add(new TermQuery(new Term("field", "term")), BooleanClause.Occur.MUST);

        assertThat(treeBuilder.collectTerms(bq))
                .containsExactly(new QueryTerm("field", "term", QueryTerm.Type.EXACT));
    }

    @Test
    public void testFieldedBooleanQuery() {

        BooleanQuery bq = new BooleanQuery();
        bq.add(new TermQuery(new Term("field1", "term1")), BooleanClause.Occur.MUST);
        bq.add(new TermQuery(new Term("field1", "term")), BooleanClause.Occur.MUST);
        FieldedBooleanQuery q = new FieldedBooleanQuery(bq);

        assertThat(treeBuilder.collectTerms(q))
                .containsExactly(new QueryTerm("field1", "term1", QueryTerm.Type.EXACT));
    }

    @Test
    public void testFilteredQueryTermExtractor() {

        Query q = new TermQuery(new Term("field", "term"));
        Filter f = new TermFilter(new Term("field", "filterterm"));
        FilteredQuery fq = new FilteredQuery(q, f);

        // treat filterquery as a conjunction, only need one subclause
        // selects 'filterterm' over 'term' because it's longer
        assertThat(treeBuilder.collectTerms(fq))
                .containsExactly(new QueryTerm("field", "filterterm", QueryTerm.Type.EXACT));

    }

    private static class MyFilter extends Filter {

        @Override
        public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
            return null;
        }
    }

    private static class MyFilterTermQueryTreeBuilder extends QueryTreeBuilder<MyFilter> {

        protected MyFilterTermQueryTreeBuilder() {
            super(MyFilter.class);
        }

        @Override
        public QueryTree buildTree(QueryAnalyzer builder, MyFilter query) {
            return new TermNode(builder.weightor, new QueryTerm("FILTER", "MYFILTER", QueryTerm.Type.EXACT));
        }
    }

    @Test
    public void testExtendedFilteredQueryExtractor() {

        QueryAnalyzer treeBuilder = getBuilder(new MyFilterTermQueryTreeBuilder());

        Query q = new RegexpQuery(new Term("FILTER", "*"));
        Filter f = new MyFilter();

        assertThat(treeBuilder.collectTerms(new FilteredQuery(q, f)))
                .containsExactly(new QueryTerm("FILTER", "MYFILTER", QueryTerm.Type.EXACT));

    }

    @Test
    public void testConstantScoreQueryExtractor() {

        BooleanQuery bq = new BooleanQuery();
        bq.add(new TermQuery(new Term("f", "q1")), BooleanClause.Occur.MUST);
        bq.add(new TermQuery(new Term("f", "q2")), BooleanClause.Occur.SHOULD);

        Query csqWithQuery = new ConstantScoreQuery(bq);
        assertThat(treeBuilder.collectTerms(csqWithQuery))
                .containsExactly(new QueryTerm("f", "q1", QueryTerm.Type.EXACT));


        TermsFilter tf = new TermsFilter(new Term("f", "q1"), new Term("f", "q22"));

        Query csqWithFilter = new ConstantScoreQuery(tf);
        assertThat(treeBuilder.collectTerms(csqWithFilter))
                .containsOnly(new QueryTerm("f", "q1", QueryTerm.Type.EXACT), new QueryTerm("f", "q22", QueryTerm.Type.EXACT));

    }

    @Test
    public void testPhraseQueryExtractor() {

        PhraseQuery pq = new PhraseQuery();
        pq.add(new Term("f", "hello"));
        pq.add(new Term("f", "encyclopedia"));

        assertThat(treeBuilder.collectTerms(pq))
                .containsOnly(new QueryTerm("f", "encyclopedia", QueryTerm.Type.EXACT));

    }

}
