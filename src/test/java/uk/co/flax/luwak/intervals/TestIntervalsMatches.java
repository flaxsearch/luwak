package uk.co.flax.luwak.intervals;

import java.io.IOException;

import com.google.common.collect.Iterables;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.fest.assertions.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import uk.co.flax.luwak.InputDocument;
import uk.co.flax.luwak.Monitor;
import uk.co.flax.luwak.MonitorQuery;
import uk.co.flax.luwak.parsers.LuceneQueryCache;
import uk.co.flax.luwak.presearcher.MatchAllPresearcher;

import static uk.co.flax.luwak.intervals.IntervalMatchesAssert.assertThat;

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
public class TestIntervalsMatches {

    static final String textfield = "textfield";

    static final Analyzer WHITESPACE = new WhitespaceAnalyzer();

    private Monitor monitor;

    public static InputDocument buildDoc(String id, String text) {
        return InputDocument.builder(id)
                .addField(textfield, text, WHITESPACE)
                .build();
    }

    @Before
    public void setUp() throws IOException {
        monitor = new Monitor(new LuceneQueryCache(textfield), new MatchAllPresearcher());
    }

    @Test
    public void singleTermQueryMatchesSingleDocument() throws IOException {

        MonitorQuery mq = new MonitorQuery("query1", "test");
        monitor.update(mq);

        IntervalsMatcher matcher = monitor.match(buildDoc("doc1", "this is a test document"), IntervalsMatcher.FACTORY);

        assertThat(matcher)
                .hasMatchCount(1)
                .matchesQuery("query1")
                    .inField(textfield)
                        .withHit(new IntervalsQueryMatch.Hit(3, 10, 3, 14));

    }

    @Test
    public void multiFieldQueryMatches() throws IOException {

        InputDocument doc = InputDocument.builder("doc1")
                .addField("field1", "this is a test of field one", WHITESPACE)
                .addField("field2", "and this is an additional test", WHITESPACE)
                .build();

        monitor.update(new MonitorQuery("query1", "field1:test field2:test"));

        IntervalsMatcher matcher = monitor.match(doc, IntervalsMatcher.FACTORY);

        assertThat(matcher)
                .hasMatchCount(1)
                .matchesQuery("query1")
                    .inField("field1")
                        .withHit(new IntervalsQueryMatch.Hit(3, 10, 3, 14))
                    .inField("field2")
                        .withHit(new IntervalsQueryMatch.Hit(5, 26, 5, 30));

    }

    @Test
    public void testHighlighterQuery() throws IOException {

        InputDocument docWithMatch = buildDoc("1", "this is a test document");
        InputDocument docWithNoMatch = buildDoc("2", "this is a document");
        InputDocument docWithNoHighlighterMatch = buildDoc("3", "this is a test");

        monitor.update(new MonitorQuery("1", "test", "document"));

        assertThat(monitor.match(docWithNoHighlighterMatch, IntervalsMatcher.FACTORY))
                .matchesQuery("1").inField(textfield)
                .withHit(new IntervalsQueryMatch.Hit(3, 10, 3, 14));

        assertThat(monitor.match(docWithMatch, IntervalsMatcher.FACTORY))
                .matchesQuery("1")
                .inField(textfield)
                .withHit(new IntervalsQueryMatch.Hit(4, 15, 4, 23));

        assertThat(monitor.match(docWithNoMatch, IntervalsMatcher.FACTORY))
                .doesNotMatchQuery("1");



    }

    @Test
    public void testHighlighterQueryOnlyReturnsHitsFromHighlighter() throws IOException {

        monitor.update(new MonitorQuery("1", "test", "document"));

        IntervalsMatcher matcher = monitor.match(buildDoc("1", "this is a test document"), IntervalsMatcher.FACTORY);

        IntervalsQueryMatch match = Iterables.getFirst(matcher, null);
        Assertions.assertThat(match).isNotNull();
        Assertions.assertThat(match.getHitCount()).isEqualTo(1);
    }


    @Test
    public void testQueryErrors() throws IOException {

        monitor.update(new MonitorQuery("1", "test"),
                       new MonitorQuery("2", "*:*"),
                       new MonitorQuery("3", "document"),
                       new MonitorQuery("4", "foo"));

        assertThat(monitor.match(buildDoc("1", "this is a test document"), IntervalsMatcher.FACTORY))
                .hasQueriesRunCount(4)
                .hasMatchCount(2)
                .hasErrorCount(1);
    }

}
