package uk.co.flax.luwak.presearcher;

import java.io.IOException;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import uk.co.flax.luwak.InputDocument;
import uk.co.flax.luwak.MonitorQuery;
import uk.co.flax.luwak.Presearcher;
import uk.co.flax.luwak.matchers.SimpleMatcher;

import static uk.co.flax.luwak.util.MatchesAssert.assertThat;

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
public abstract class FieldFilterPresearcherComponentTestBase extends PresearcherTestBase {

    public static class TestTermFiltered extends FieldFilterPresearcherComponentTestBase {

        @Override
        protected Presearcher createPresearcher() {
            return new TermFilteredPresearcher(new FieldFilterPresearcherComponent("language"));
        }
    }

    public static class TestMultipass extends FieldFilterPresearcherComponentTestBase {

        @Override
        protected Presearcher createPresearcher() {
            return new MultipassTermFilteredPresearcher(2, 0.0f, new FieldFilterPresearcherComponent("language"));
        }
    }

    @Test
    public void testFieldFiltering() throws IOException {

        monitor.update(new MonitorQuery("1", "test", ImmutableMap.of("language", "en")),
                       new MonitorQuery("2", "test", ImmutableMap.of("language", "de")),
                       new MonitorQuery("3", "wibble", ImmutableMap.of("language", "en")),
                       new MonitorQuery("4", "*:*", ImmutableMap.of("language", "de")));

        InputDocument enDoc = InputDocument.builder("enDoc")
                .addField(TEXTFIELD, "this is a test", WHITESPACE)
                .addField("language", "en", WHITESPACE)
                .build();
        assertThat(monitor.match(enDoc, SimpleMatcher.FACTORY))
                .matchesQuery("1")
                .hasMatchCount(1)
                .hasQueriesRunCount(1);

        InputDocument deDoc = InputDocument.builder("deDoc")
                .addField(TEXTFIELD, "das ist ein test", WHITESPACE)
                .addField("language", "de", WHITESPACE)
                .build();
        assertThat(monitor.match(deDoc, SimpleMatcher.FACTORY))
                .matchesQuery("2")
                .matchesQuery("4")
                .hasMatchCount(2)
                .hasQueriesRunCount(2);

        InputDocument bothDoc = InputDocument.builder("bothDoc")
                .addField(TEXTFIELD, "this is ein test", WHITESPACE)
                .addField("language", "en", WHITESPACE)
                .addField("language", "de", WHITESPACE)
                .build();
        assertThat(monitor.match(bothDoc, SimpleMatcher.FACTORY))
                .matchesQuery("1")
                .matchesQuery("2")
                .matchesQuery("4")
                .hasMatchCount(3)
                .hasQueriesRunCount(3);
    }

    @Test
    public void testFilteringOnMatchAllQueries() throws IOException {
        monitor.update(new MonitorQuery("1", "*:*", ImmutableMap.of("language", "de")));

        InputDocument enDoc = InputDocument.builder("enDoc")
                .addField(TEXTFIELD, "this is a test", WHITESPACE)
                .addField("language", "en", WHITESPACE)
                .build();
        assertThat(monitor.match(enDoc, SimpleMatcher.FACTORY))
                .hasMatchCount(0)
                .hasQueriesRunCount(0);
    }

}
