package uk.co.flax.luwak.presearcher;

import com.google.common.collect.ImmutableSet;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.Version;
import org.junit.Test;
import uk.co.flax.luwak.InputDocument;
import uk.co.flax.luwak.Presearcher;

import java.util.Set;

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

public class TestPresearcherCustomFiltering {

    @Test
    public void testCanFilterOutFields() {

        InputDocument doc = InputDocument.builder("id")
                .addField("field1", "foo", new KeywordAnalyzer())
                .addField("field2", "bar", new KeywordAnalyzer())
                .build();

        Presearcher presearcher = new TermFilteredPresearcher(new DocumentTokenFilter.FieldFilter("field2"));
        BooleanQuery q = (BooleanQuery) presearcher.buildQuery(doc);

        assertThat(q.clauses())
                .containsExactly(clause("field1", "foo"), clause("field1", "__ANYTOKEN__"));

    }

    @Test
    public void testCanFilterOutTokens() {

        InputDocument doc = InputDocument.builder("id")
                .addField("field1", "foo bar baz", new WhitespaceAnalyzer(Version.LUCENE_50))
                .build();

        Set<String> tokensToFilter = ImmutableSet.of("bar");

        Presearcher presearcher = new TermFilteredPresearcher(new DocumentTokenFilter.TokensFilter(tokensToFilter));
        BooleanQuery q = (BooleanQuery) presearcher.buildQuery(doc);

        assertThat(q.clauses())
                .containsOnly(clause("field1", "__ANYTOKEN__"), clause("field1", "foo"), clause("field1", "baz"));

    }

    @Test
    public void testCanFilterOutTokensFromSpecificFields() {

        InputDocument doc = InputDocument.builder("id")
                .addField("field1", "foo bar baz", new WhitespaceAnalyzer(Version.LUCENE_50))
                .addField("field2", "bar", new KeywordAnalyzer())
                .build();

        Set<String> tokensToFilter = ImmutableSet.of("bar");

        Presearcher presearcher
                = new TermFilteredPresearcher(new DocumentTokenFilter.FieldTokensFilter("field1", tokensToFilter));
        BooleanQuery q = (BooleanQuery) presearcher.buildQuery(doc);

        assertThat(q.clauses())
                .containsOnly(clause("field1", "__ANYTOKEN__"), clause("field2", "__ANYTOKEN__"), clause("field1", "foo"),
                              clause("field1", "baz"), clause("field2", "bar"));


    }

    private static BooleanClause clause(String field, String value) {
        return new BooleanClause(new TermQuery(new Term(field, value)), BooleanClause.Occur.SHOULD);
    }

}
