package uk.co.flax.luwak.termextractor;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.spans.*;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

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
public class TestSpanExtractors {

    private static final QueryAnalyzer treeBuilder = new QueryAnalyzer();

    @Test
    public void testOrderedNearExtractor() {
        SpanNearQuery q = new SpanNearQuery(new SpanQuery[]{
                new SpanTermQuery(new Term("field1", "term1")),
                new SpanTermQuery(new Term("field1", "term"))
        }, 0, true);

        assertThat(treeBuilder.collectTerms(q))
                .containsExactly(new QueryTerm("field1", "term1", QueryTerm.Type.EXACT));
    }

    @Test
    public void testOrderedNearWithWildcardExtractor() {
        SpanNearQuery q = new SpanNearQuery(new SpanQuery[]{
                new SpanMultiTermQueryWrapper<>(new RegexpQuery(new Term("field", "super.*cali.*"))),
                new SpanTermQuery(new Term("field", "is"))
        }, 0, true);

        assertThat(treeBuilder.collectTerms(q)).containsExactly(new QueryTerm("field", "is", QueryTerm.Type.EXACT));
    }

    @Test
    public void testSpanOrExtractor() {
        SpanOrQuery or = new SpanOrQuery(new SpanTermQuery(new Term("field", "term1")),
                                         new SpanTermQuery(new Term("field", "term2")));
        assertThat(treeBuilder.collectTerms(or)).containsOnly(
                new QueryTerm("field", "term1", QueryTerm.Type.EXACT),
                new QueryTerm("field", "term2", QueryTerm.Type.EXACT)
        );
    }

    @Test
    public void testSpanMultiTerms() {
        SpanQuery q = new SpanMultiTermQueryWrapper<>(new RegexpQuery(new Term("field", "term.*")));
        assertThat(treeBuilder.collectTerms(q))
                .hasSize(1)
                .extracting("type")
                .containsOnly(QueryTerm.Type.ANY);
    }

}
