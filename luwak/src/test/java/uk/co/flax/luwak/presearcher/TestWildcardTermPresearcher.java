package uk.co.flax.luwak.presearcher;

import java.io.IOException;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.junit.Test;
import uk.co.flax.luwak.InputDocument;
import uk.co.flax.luwak.MonitorQuery;
import uk.co.flax.luwak.Presearcher;
import uk.co.flax.luwak.UpdateException;
import uk.co.flax.luwak.assertions.TokenStreamAssert;
import uk.co.flax.luwak.matchers.SimpleMatcher;

import static uk.co.flax.luwak.assertions.MatchesAssert.assertThat;

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

public class TestWildcardTermPresearcher extends PresearcherTestBase {

    @Test
    public void filtersWildcards() throws IOException, UpdateException {

        monitor.update(new MonitorQuery("1", "/hell.*/"));

        assertThat(monitor.match(buildDoc("doc1", TEXTFIELD, "well hello there"), SimpleMatcher.FACTORY))
                .hasMatchCount("doc1", 1);

    }

    @Test
    public void ngramsOnlyMatchWildcards() throws IOException, UpdateException {

        monitor.update(new MonitorQuery("1", "hello"));

        assertThat(monitor.match(buildDoc("doc1", TEXTFIELD, "hellopolis"), SimpleMatcher.FACTORY))
                .hasQueriesRunCount(0);

    }

    @Test
    public void testLongTermsStillMatchWildcards() throws IOException, UpdateException {

        monitor.update(new MonitorQuery("1", "/a.*/"));

        InputDocument doc1 = InputDocument.builder("doc1")
                .addField(TEXTFIELD, Strings.repeat("a", WildcardNGramPresearcherComponent.DEFAULT_MAX_TOKEN_SIZE + 1), new KeywordAnalyzer())
                .build();

        assertThat(monitor.match(doc1, SimpleMatcher.FACTORY))
                .hasQueriesRunCount(1)
                .matchesQuery("1", "doc1");

    }

    @Test
    public void caseSensitivity() throws IOException, UpdateException {

        monitor.update(new MonitorQuery("1", "foo"));

        assertThat(monitor.match(buildDoc("doc1", TEXTFIELD, "Foo foo"), SimpleMatcher.FACTORY))
                .hasMatchCount("doc1", 1);

    }

    @Override
    protected Presearcher createPresearcher() {
        return new TermFilteredPresearcher(new WildcardNGramPresearcherComponent());
    }

    @Test
    public void testPresearcherComponent() throws IOException {

        PresearcherComponent comp
                = new WildcardNGramPresearcherComponent("FOO", 10, "__wibble__", Sets.newHashSet("field1"));

        try (Analyzer input = new WhitespaceAnalyzer()) {

            // field1 is in the excluded set, so nothing should happen
            TokenStreamAssert.assertThat(comp.filterDocumentTokens("field1", input.tokenStream("field1", "hello world")))
                    .nextEquals("hello")
                    .nextEquals("world")
                    .isExhausted();

            // field2 is not excluded
            TokenStreamAssert.assertThat(comp.filterDocumentTokens("field", input.tokenStream("field", "harm alarm asdasasdasdasd")))
                    .nextEquals("harm")
                    .nextEquals("harmFOO").nextEquals("harFOO").nextEquals("haFOO").nextEquals("hFOO")
                    .nextEquals("armFOO").nextEquals("arFOO").nextEquals("aFOO")
                    .nextEquals("rmFOO").nextEquals("rFOO")
                    .nextEquals("mFOO")
                    .nextEquals("FOO")
                    .nextEquals("alarm")
                    .nextEquals("alarmFOO").nextEquals("alarFOO").nextEquals("alaFOO").nextEquals("alFOO")
                    .nextEquals("larmFOO").nextEquals("larFOO").nextEquals("laFOO").nextEquals("lFOO")
                    .nextEquals("asdasasdasdasd")
                    .nextEquals("__wibble__")
                    .isExhausted();
        }
    }

    @Test
    public void testUnionRegexp() throws IOException, UpdateException {

        monitor.update(new MonitorQuery("1", "/play(er|ed)/"));

        assertThat(monitor.match(buildDoc("doc1", TEXTFIELD, "player or played"), SimpleMatcher.FACTORY))
                    .hasMatchCount("doc1", 1);

        assertThat(monitor.match(buildDoc("doc1", TEXTFIELD, "players playing"), SimpleMatcher.FACTORY))
                .hasQueriesRunCount(1)
                .hasMatchCount("doc1", 0);
    }

    @Test
    public void testCharsetRegexp() throws IOException, UpdateException {

        monitor.update(new MonitorQuery("1", "/201[0-9]/"));

        assertThat(monitor.match(buildDoc("doc1", TEXTFIELD, "the year 2017"), SimpleMatcher.FACTORY))
                    .hasMatchCount("doc1", 1);

        assertThat(monitor.match(buildDoc("doc1", TEXTFIELD, "The number 201"), SimpleMatcher.FACTORY))
                .hasQueriesRunCount(1)
                .hasMatchCount("doc1", 0);
    }

    @Test
    public void testEscapedCharRegexp() throws IOException, UpdateException {

        monitor.update(new MonitorQuery("1", "/amazon\\.c.m/"));

        assertThat(monitor.match(buildDoc("doc1", TEXTFIELD, "amazon.com"), SimpleMatcher.FACTORY))
                .hasMatchCount("doc1", 1);

        assertThat(monitor.match(buildDoc("doc1", TEXTFIELD, "amazon com"), SimpleMatcher.FACTORY))
            .hasQueriesRunCount(1)
            .hasMatchCount("doc1", 0);
    }

}
