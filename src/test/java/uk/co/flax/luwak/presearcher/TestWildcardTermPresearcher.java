package uk.co.flax.luwak.presearcher;

import java.io.IOException;

import com.google.common.base.Strings;
import org.junit.Test;
import uk.co.flax.luwak.InputDocument;
import uk.co.flax.luwak.MonitorQuery;
import uk.co.flax.luwak.Presearcher;
import uk.co.flax.luwak.matchers.SimpleMatcher;

import static uk.co.flax.luwak.util.MatchesAssert.assertThat;

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
    public void filtersWildcards() throws IOException {

        monitor.update(new MonitorQuery("1", "/hell.*/"));

        InputDocument doc1 = InputDocument.builder("doc1")
                .addField(TEXTFIELD, "well hello there", WHITESPACE)
                .build();

        assertThat(monitor.match(doc1, SimpleMatcher.FACTORY))
                .hasMatchCount(1);

    }

    @Test
    public void ngramsOnlyMatchWildcards() throws IOException {

        monitor.update(new MonitorQuery("1", "hello"));

        InputDocument doc1 = InputDocument.builder("doc1")
                .addField(TEXTFIELD, "hellopolis", WHITESPACE)
                .build();

        assertThat(monitor.match(doc1, SimpleMatcher.FACTORY))
                .hasQueriesRunCount(0);

    }

    @Test
    public void testLongTermsStillMatchWildcards() throws IOException {

        monitor.update(new MonitorQuery("1", "/a.*/"));

        InputDocument doc1 = InputDocument.builder("doc1")
                .addField(TEXTFIELD, Strings.repeat("a", WildcardNGramPresearcherComponent.DEFAULT_MAX_TOKEN_SIZE + 1), WHITESPACE)
                .build();

        assertThat(monitor.match(doc1, SimpleMatcher.FACTORY))
                .hasQueriesRunCount(1)
                .matchesQuery("1");

    }

    @Test
    public void caseSensitivity() throws IOException {

        monitor.update(new MonitorQuery("1", "foo"));

        InputDocument doc1 = InputDocument.builder("doc1")
                .addField(TEXTFIELD, "Foo foo", WHITESPACE)
                .build();

        assertThat(monitor.match(doc1, SimpleMatcher.FACTORY))
                .hasMatchCount(1);

    }

    @Override
    protected Presearcher createPresearcher() {
        return new TermFilteredPresearcher(new WildcardNGramPresearcherComponent());
    }
}
