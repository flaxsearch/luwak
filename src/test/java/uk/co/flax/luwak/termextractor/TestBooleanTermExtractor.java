package uk.co.flax.luwak.termextractor;

import java.util.List;

import org.apache.lucene.search.Query;
import org.junit.Test;
import uk.co.flax.luwak.presearcher.PresearcherComponent;
import uk.co.flax.luwak.util.ParserUtils;

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

    private static final QueryAnalyzer treeBuilder = PresearcherComponent.buildQueryAnalyzer();

    @Test
    public void allDisjunctionQueriesAreIncluded() throws Exception {

        Query bq = ParserUtils.parse("field1:term1 field1:term2");
        List<QueryTerm> terms = treeBuilder.collectTerms(bq);

        assertThat(terms).containsOnly(
                new QueryTerm("field1", "term1", QueryTerm.Type.EXACT),
                new QueryTerm("field1", "term2", QueryTerm.Type.EXACT));

    }

    @Test
    public void allNestedDisjunctionClausesAreIncluded() throws Exception {

        Query q = ParserUtils.parse("field1:term3 (field1:term1 field1:term2)");

        assertThat(treeBuilder.collectTerms(q)).hasSize(3);
    }

    @Test
    public void allDisjunctionClausesOfAConjunctionAreExtracted() throws Exception {

        Query q = ParserUtils.parse("+(field1:term1 field1:term2) field1:term3");

        assertThat(treeBuilder.collectTerms(q)).hasSize(2);

    }

    @Test
    public void conjunctionsOutweighDisjunctions() throws Exception {
        Query bq = ParserUtils.parse("field1:term1 +field1:term2");

        assertThat(treeBuilder.collectTerms(bq))
                .containsOnly(new QueryTerm("field1", "term2", QueryTerm.Type.EXACT));
    }

    @Test
    public void disjunctionsWithPureNegativeClausesReturnANYTOKEN() throws Exception {

        Query q = ParserUtils.parse("+field1:term1 +(field2:term22 (-field2:notterm))");

        assertThat(treeBuilder.collectTerms(q))
                .containsOnly(new QueryTerm("field1", "term1", QueryTerm.Type.EXACT));

    }

    @Test
    public void disjunctionsWithMatchAllNegativeClausesReturnANYTOKEN() throws Exception {

        Query q = ParserUtils.parse("+field1:term1 +(field2:term22 (*:* -field2:notterm))");

        assertThat(treeBuilder.collectTerms(q))
                .containsOnly(new QueryTerm("field1", "term1", QueryTerm.Type.EXACT));

    }

}
