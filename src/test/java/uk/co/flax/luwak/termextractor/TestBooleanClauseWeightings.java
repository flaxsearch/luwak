package uk.co.flax.luwak.termextractor;

import org.apache.lucene.search.Query;
import org.junit.Test;
import uk.co.flax.luwak.util.ParserUtils;

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
public class TestBooleanClauseWeightings {

    private static QueryAnalyzer treeBuilder = new QueryAnalyzer();

    @Test
    public void exactClausesPreferred() throws Exception {

        Query bq = ParserUtils.parse("+field2:[1 TO 2] +(field1:term1 field1:term2)");

        assertThat(treeBuilder.collectTerms(bq))
                .hasSize(2);
    }

    @Test
    public void longerTermsPreferred() throws Exception {

        Query q = ParserUtils.parse("field1:(+a +supercalifragilisticexpialidocious +b)");

        assertThat(treeBuilder.collectTerms(q))
                .containsExactly(new QueryTerm("field1", "supercalifragilisticexpialidocious", QueryTerm.Type.EXACT));
    }

}
