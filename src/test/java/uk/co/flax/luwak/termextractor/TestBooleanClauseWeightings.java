package uk.co.flax.luwak.termextractor;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.TermQuery;
import org.junit.Test;
import uk.co.flax.luwak.presearcher.PresearcherComponent;

import static org.fest.assertions.api.Assertions.assertThat;
import static uk.co.flax.luwak.termextractor.BooleanQueryUtils.newTermQuery;

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
public class TestBooleanClauseWeightings {

    private static QueryAnalyzer treeBuilder = PresearcherComponent.buildQueryAnalyzer();

    @Test
    public void exactClausesPreferred() {
        BooleanQuery bq = BooleanQueryUtils.BQBuilder.newBQ()
                .addMustClause(NumericRangeQuery.newIntRange("field2", 1, 2, false, false))
                .addMustClause(BooleanQueryUtils.BQBuilder.newBQ()
                                .addShouldClause(newTermQuery("field1", "term1"))
                                .addShouldClause(newTermQuery("field1", "term2"))
                                .build()
                )
                .build();

        assertThat(treeBuilder.collectTerms(bq))
                .hasSize(2);
    }

    @Test
    public void longerTermsPreferred() {
        BooleanQuery bq = BooleanQueryUtils.BQBuilder.newBQ()
                .addMustClause(new TermQuery(new Term("field1", "a")))
                .addMustClause(new TermQuery(new Term("field1", "supercalifragilisticexpialidocious")))
                .addMustClause(new TermQuery(new Term("field1", "b")))
                .build();

        assertThat(treeBuilder.collectTerms(bq))
                .containsExactly(new QueryTerm("field1", "supercalifragilisticexpialidocious", QueryTerm.Type.EXACT));
    }

}
