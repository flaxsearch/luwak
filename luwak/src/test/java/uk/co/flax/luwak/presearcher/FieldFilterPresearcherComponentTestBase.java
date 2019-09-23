package uk.co.flax.luwak.presearcher;

import java.io.IOException;

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import uk.co.flax.luwak.*;
import uk.co.flax.luwak.matchers.SimpleMatcher;

import static org.assertj.core.api.Fail.fail;
import static uk.co.flax.luwak.assertions.MatchesAssert.assertThat;

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

    public static final Analyzer ANALYZER = new StandardAnalyzer();

    @Test
    public void testBatchFiltering() throws IOException, UpdateException {

        monitor.update(new MonitorQuery("1", "test", ImmutableMap.of("language", "en")),
                new MonitorQuery("2", "wahl", ImmutableMap.of("language", "de")),
                new MonitorQuery("3", "wibble", ImmutableMap.of("language", "en")),
                new MonitorQuery("4", "*:*", ImmutableMap.of("language", "de")),
                new MonitorQuery("5", "*:*", ImmutableMap.of("language", "es")));

        DocumentBatch enBatch = DocumentBatch.of(
                InputDocument.builder("en1")
                        .addField(TEXTFIELD, "this is a test", ANALYZER)
                        .addField("language", "en", ANALYZER)
                        .build(),
                InputDocument.builder("en2")
                        .addField(TEXTFIELD, "this is a wibble", ANALYZER)
                        .addField("language", "en", ANALYZER)
                        .build(),
                InputDocument.builder("en3")
                        .addField(TEXTFIELD, "wahl is a misspelling of whale", ANALYZER)
                        .addField("language", "en", ANALYZER)
                        .build()
        );
        assertThat(monitor.match(enBatch, SimpleMatcher.FACTORY))
                .hasMatchCount("en1", 1)
                .matchesQuery("1", "en1")
                .hasMatchCount("en2", 1)
                .matchesQuery("3", "en2")
                .hasMatchCount("en3", 0)
                .hasQueriesRunCount(2);
    }

    @Test
    public void testBatchesWithDissimilarFieldValuesThrowExceptions() throws IOException {

        DocumentBatch batch = DocumentBatch.of(
                InputDocument.builder("1").addField(TEXTFIELD, "test", ANALYZER).addField("language", "en", ANALYZER).build(),
                InputDocument.builder("2").addField(TEXTFIELD, "test", ANALYZER).addField("language", "de", ANALYZER).build()
        );

        try {
            monitor.match(batch, SimpleMatcher.FACTORY);
            fail("Expected an IllegalArgumentException for mixed-filter-field-value DocumentBatch");
        }
        catch (IllegalArgumentException e) {
            Assertions.assertThat(e.getMessage()).contains("language:");
        }

    }

    @Test
    public void testFieldFiltering() throws IOException, UpdateException {

        monitor.update(new MonitorQuery("1", "test", ImmutableMap.of("language", "en")),
                       new MonitorQuery("2", "test", ImmutableMap.of("language", "de")),
                       new MonitorQuery("3", "wibble", ImmutableMap.of("language", "en")),
                       new MonitorQuery("4", "*:*", ImmutableMap.of("language", "de")));

        InputDocument enDoc = InputDocument.builder("enDoc")
                .addField(TEXTFIELD, "this is a test", ANALYZER)
                .addField("language", "en", ANALYZER)
                .build();

        assertThat(monitor.match(enDoc, SimpleMatcher.FACTORY))
                .matchesQuery("1", "enDoc")
                .hasMatchCount("enDoc", 1)
                .hasQueriesRunCount(1);

        InputDocument deDoc = InputDocument.builder("deDoc")
                .addField(TEXTFIELD, "das ist ein test", ANALYZER)
                .addField("language", "de", ANALYZER)
                .build();
        assertThat(monitor.match(deDoc, SimpleMatcher.FACTORY))
                .matchesQuery("2", "deDoc")
                .matchesQuery("4", "deDoc")
                .hasMatchCount("deDoc", 2)
                .hasQueriesRunCount(2);

        InputDocument bothDoc = InputDocument.builder("bothDoc")
                .addField(TEXTFIELD, "this is ein test", ANALYZER)
                .addField("language", "en", ANALYZER)
                .addField("language", "de", ANALYZER)
                .build();
        assertThat(monitor.match(bothDoc, SimpleMatcher.FACTORY))
                .matchesQuery("1", "bothDoc")
                .matchesQuery("2", "bothDoc")
                .matchesQuery("4", "bothDoc")
                .hasMatchCount("bothDoc", 3)
                .hasQueriesRunCount(3);

    }

    @Test
    public void testFilteringOnMatchAllQueries() throws IOException, UpdateException {
        monitor.update(new MonitorQuery("1", "*:*", ImmutableMap.of("language", "de")));

        InputDocument doc = InputDocument.builder("enDoc")
                .addField(TEXTFIELD, "this is a test", ANALYZER)
                .addField("language", "en", ANALYZER)
                .build();
        assertThat(monitor.match(doc, SimpleMatcher.FACTORY))
                .hasMatchCount("enDoc", 0)
                .hasQueriesRunCount(0);
    }

    @Test
    public void testDebugQueries() throws Exception {

        monitor.update(new MonitorQuery("1", "test", ImmutableMap.of("language", "en")));

        InputDocument doc = InputDocument.builder("enDoc")
                .addField(TEXTFIELD, "this is a test", ANALYZER)
                .addField("language", "en", ANALYZER)
                .build();

        PresearcherMatches<QueryMatch> matches = monitor.debug(doc, SimpleMatcher.FACTORY);
        Assertions.assertThat(matches.match("1", "enDoc").presearcherMatches).isNotEmpty();
        System.out.println(matches.match("1", "enDoc").presearcherMatches);

    }

}
