package uk.co.flax.luwak;

import com.google.common.collect.ImmutableList;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.junit.Test;
import uk.co.flax.luwak.testutils.ParserUtils;

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

public class TestQueryDecomposer {

    public static final QueryDecomposer decomposer = new QueryDecomposer();

    public static Query q(String q) throws Exception {
        return ParserUtils.parse(q);
    }

    @Test
    public void testConjunctionsAreNotDecomposed() throws Exception {
        Query q = ParserUtils.parse("+hello world");
        assertThat(decomposer.decompose(q))
                .hasSize(1)
                .containsExactly(q("+hello world"));
    }

    @Test
    public void testSimpleDisjunctions() throws Exception {
        Query q = ParserUtils.parse("hello world");
        assertThat(decomposer.decompose(q))
                .hasSize(2)
                .containsExactly(q("hello"), q("world"));
    }

    @Test
    public void testNestedDisjunctions() throws Exception {
        Query q = ParserUtils.parse("(hello goodbye) world");
        assertThat(decomposer.decompose(q))
                .hasSize(3)
                .containsExactly(q("hello"), q("goodbye"), q("world"));
    }

    @Test
    public void testExclusions() throws Exception {
        assertThat(decomposer.decompose(q("hello world -goodbye")))
                .containsExactly(q("+hello -goodbye"), q("+world -goodbye"));
    }

    @Test
    public void testNestedExclusions() throws Exception {
        assertThat(decomposer.decompose(q("((hello world) -goodbye) -greeting")))
                .containsExactly(q("+(+hello -goodbye) -greeting"),
                        q("+(+world -goodbye) -greeting"));
    }

    @Test
    public void testSingleValuedConjunctions() throws Exception {
        assertThat(decomposer.decompose(q("+(hello world)")))
                .containsExactly(q("hello"), q("world"));
    }

    @Test
    public void testSingleValuedConjunctWithExclusions() throws Exception {
        assertThat(decomposer.decompose(q("+(hello world) -goodbye")))
                .containsExactly(q("+hello -goodbye"), q("+world -goodbye"));
    }

    @Test
    public void testBoostsArePreserved() throws Exception {
        assertThat(decomposer.decompose(q("+(hello world)^0.7")))
                .containsExactly(q("hello^0.7"), q("world^0.7"));
        assertThat(decomposer.decompose(q("+(hello world)^0.7 -goodbye")))
                .containsExactly(q("+hello^0.7 -goodbye"), q("+world^0.7 -goodbye"));
        assertThat(decomposer.decompose(q("+(hello^0.5 world)^0.8")))
                .containsExactly(q("(hello^0.5)^0.8"), q("world^0.8"));
    }

    @Test
    public void testDisjunctionMaxDecomposition() throws Exception {
        Query q = new DisjunctionMaxQuery(
                ImmutableList.<Query>of(new TermQuery(new Term("f", "t1")), new TermQuery(new Term("f", "t2"))), 0.1f
        );

        assertThat(decomposer.decompose(q)).containsExactly(q("f:t1"), q("f:t2"));
    }

    @Test
    public void testNestedDisjunctionMaxDecomposition() throws Exception {
        Query q = new DisjunctionMaxQuery(
                ImmutableList.<Query>of(q("hello goodbye"), q("world")), 0.1f
        );

        assertThat(decomposer.decompose(q))
                .hasSize(3)
                .containsExactly(q("hello"), q("goodbye"), q("world"));
    }

    @Test
    public void testFilterAndShouldClause() throws Exception {
        final Query shouldTermQuery = new TermQuery(new Term("f", "should"));
        final Query filterTermQuery = new TermQuery(new Term("f", "filter"));
        Query q = new BooleanQuery.Builder()
                .add(shouldTermQuery, BooleanClause.Occur.SHOULD)
                .add(filterTermQuery, BooleanClause.Occur.FILTER)
                .build();

        assertThat(decomposer.decompose(q))
                .hasSize(1)
                .containsExactly(q);
    }
}
