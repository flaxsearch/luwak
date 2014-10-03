package uk.co.flax.luwak.termextractor;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.intervals.FieldedBooleanQuery;
import org.apache.lucene.search.intervals.OrderedNearQuery;
import org.apache.lucene.search.intervals.UnorderedNearQuery;
import org.junit.Test;
import uk.co.flax.luwak.presearcher.IntervalsPresearcherComponent;
import uk.co.flax.luwak.presearcher.PresearcherComponent;

import static org.fest.assertions.api.Assertions.assertThat;

/**
 * Copyright (c) 2014 Lemur Consulting Ltd.
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
public class TestIntervalExtractors {

    private static final QueryAnalyzer treeBuilder
            = PresearcherComponent.buildQueryAnalyzer(new IntervalsPresearcherComponent());

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
    public void testFieldedBooleanQuery() {

        BooleanQuery bq = new BooleanQuery();
        bq.add(new TermQuery(new Term("field1", "term1")), BooleanClause.Occur.MUST);
        bq.add(new TermQuery(new Term("field1", "term")), BooleanClause.Occur.MUST);
        FieldedBooleanQuery q = new FieldedBooleanQuery(bq);

        assertThat(treeBuilder.collectTerms(q))
                .containsExactly(new QueryTerm("field1", "term1", QueryTerm.Type.EXACT));
    }
}
