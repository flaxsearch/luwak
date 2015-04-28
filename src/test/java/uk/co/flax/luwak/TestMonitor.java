package uk.co.flax.luwak;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import uk.co.flax.luwak.matchers.SimpleMatcher;
import uk.co.flax.luwak.presearcher.MatchAllPresearcher;
import uk.co.flax.luwak.queryparsers.LuceneQueryParser;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
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

public class TestMonitor {

    static final String TEXTFIELD = "TEXTFIELD";

    static final Analyzer ANALYZER = new KeywordAnalyzer();

    private Monitor monitor;

    @Before
    public void setUp() throws IOException {
        monitor = new Monitor(new LuceneQueryParser(TEXTFIELD, ANALYZER), new MatchAllPresearcher());
    }

    @Test
    public void singleTermQueryMatchesSingleDocument() throws IOException {

        String document = "This is a test document";
        InputDocument doc = InputDocument.builder("doc1")
                .addField(TEXTFIELD, document, WHITESPACE)
                .build();

        monitor.update(new MonitorQuery("query1", "test"));

        assertThat(monitor.match(doc, SimpleMatcher.FACTORY))
                .matches("doc1")
                .hasMatchCount(1)
                .matchesQuery("query1");

    }

    @Test
    public void matchStatisticsAreReported() throws IOException {
        String document = "This is a test document";
        InputDocument doc = InputDocument.builder("doc1")
                .addField(TEXTFIELD, document, WHITESPACE)
                .build();

        monitor.update(new MonitorQuery("query1", "test"));

        Matches<QueryMatch> matches = monitor.match(doc, SimpleMatcher.FACTORY);
        Assertions.assertThat(matches.getQueriesRun()).isEqualTo(1);
        Assertions.assertThat(matches.getQueryBuildTime()).isGreaterThan(-1);
        Assertions.assertThat(matches.getSearchTime()).isGreaterThan(-1);
    }

    @Test
    public void updatesOverwriteOldQueries() throws IOException {
        monitor.update(new MonitorQuery("query1", "this"));

        monitor.update(new MonitorQuery("query1", "that"));

        InputDocument doc = InputDocument.builder("doc1").addField(TEXTFIELD, "that", WHITESPACE).build();
        assertThat(monitor.match(doc, SimpleMatcher.FACTORY))
                .hasQueriesRunCount(1)
                .matchesQuery("query1");
    }

    @Test
    public void canAddEmptyHighlightQueries() throws IOException {
        monitor.update(new MonitorQuery("1", "query", ""));
        Assertions.assertThat(monitor.getQueryCount()).isEqualTo(1);
    }

    @Test
    public void canDeleteById() throws IOException {

        monitor.update(new MonitorQuery("query1", "this"));
        monitor.update(new MonitorQuery("query2", "that"), new MonitorQuery("query3", "other"));
        Assertions.assertThat(monitor.getQueryCount()).isEqualTo(3);

        monitor.deleteById("query2", "query1");
        Assertions.assertThat(monitor.getQueryCount()).isEqualTo(1);

        InputDocument doc = InputDocument.builder("doc1").addField(TEXTFIELD, "other things", WHITESPACE).build();
        assertThat(monitor.match(doc, SimpleMatcher.FACTORY))
                .hasQueriesRunCount(1)
                .matchesQuery("query3");

    }

    @Test
    public void canRetrieveQuery() throws IOException {

        monitor.update(new MonitorQuery("query1", "this"), new MonitorQuery("query2", "that", "hl"));
        Assertions.assertThat(monitor.getQueryCount()).isEqualTo(2);

        MonitorQuery mq = monitor.getQuery("query2");
        Assertions.assertThat(mq).isEqualTo(new MonitorQuery("query2", "that", "hl"));

    }

    @Test
    public void canClearTheMonitor() throws IOException {
        monitor.update(new MonitorQuery("query1", "a"), new MonitorQuery("query2", "b"), new MonitorQuery("query3", "c"));
        Assertions.assertThat(monitor.getQueryCount()).isEqualTo(3);

        monitor.clear();
        Assertions.assertThat(monitor.getQueryCount()).isEqualTo(0);
    }

    @Test
    public void testMatchesAgainstAnEmptyMonitor() throws IOException {

        monitor.clear();
        Assertions.assertThat(monitor.getQueryCount()).isEqualTo(0);

        InputDocument doc = InputDocument.builder("doc1").addField(TEXTFIELD, "other things", WHITESPACE).build();
        Matches<QueryMatch> matches = monitor.match(doc, SimpleMatcher.FACTORY);

        Assertions.assertThat(matches.getQueriesRun()).isEqualTo(0);
    }

    @Test
    public void testUpdateReporting() throws IOException {

        Monitor.UpdateReporter reporter = mock(Monitor.UpdateReporter.class);
        List<MonitorQuery> queries = new ArrayList<>(10400);
        for (int i = 0; i < 10355; i++) {
            queries.add(new MonitorQuery(Integer.toString(i), "test"));
        }

        monitor.update(queries, reporter);
        verify(reporter).progress(5001, 5001);
        verify(reporter).progress(10002, 5001);
        verify(reporter).finish(10355, 353);

    }

    static final Analyzer WHITESPACE = new WhitespaceAnalyzer();

}
