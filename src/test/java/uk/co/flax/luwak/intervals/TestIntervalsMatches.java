package uk.co.flax.luwak.intervals;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.Version;
import org.junit.Before;
import org.junit.Test;
import uk.co.flax.luwak.InputDocument;
import uk.co.flax.luwak.Monitor;
import uk.co.flax.luwak.MonitorQuery;
import uk.co.flax.luwak.impl.MatchAllPresearcher;
import uk.co.flax.luwak.parsers.LuceneQueryParser;

import java.io.IOException;
import java.util.List;

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
public class TestIntervalsMatches {

    static final String textfield = "textfield";

    static final Analyzer WHITESPACE = new WhitespaceAnalyzer(Version.LUCENE_50);

    private Monitor monitor;

    public static InputDocument buildDoc(String id, String text) {
        return InputDocument.builder(id)
                .addField(textfield, text, WHITESPACE)
                .build();
    }

    @Before
    public void setUp() throws IOException {
        monitor = new Monitor(new LuceneQueryParser(textfield), new MatchAllPresearcher());
    }

    @Test
    public void singleTermQueryMatchesSingleDocument() throws IOException {

        MonitorQuery mq = new MonitorQuery("query1", "test");
        monitor.update(mq);

        InputDocument doc = buildDoc("doc1", "This is a test document");
        IntervalsMatcher matcher = new IntervalsMatcher();
        monitor.match(doc, matcher);

        List<IntervalsQueryMatch> matches = matcher.getMatches();
        assertThat(matches).hasSize(1);
        assertThat(matches.get(0).getQueryId()).isEqualTo("query1");
        assertThat(matches.get(0).getHitCount()).isEqualTo(1);
        assertThat(matches.get(0).getHits(textfield)).containsExactly(new IntervalsQueryMatch.Hit(3, 10, 3, 14));

    }

    @Test
    public void multiFieldQueryMatches() throws IOException {

        InputDocument doc = InputDocument.builder("doc1")
                .addField("field1", "this is a test of field one", WHITESPACE)
                .addField("field2", "and this is an additional test", WHITESPACE)
                .build();

        monitor.update(new MonitorQuery("query1", "field1:test field2:test"));

        IntervalsMatcher matcher = new IntervalsMatcher();
        monitor.match(doc, matcher);

        List<IntervalsQueryMatch> matches = matcher.getMatches();
        assertThat(matches).hasSize(1);
        assertThat(matches.get(0).getQueryId()).isEqualTo("query1");
        assertThat(matches.get(0).getHitCount()).isEqualTo(2);
        assertThat(matches.get(0).getHits("field1")).containsExactly(new IntervalsQueryMatch.Hit(3, 10, 3, 14));
        assertThat(matches.get(0).getHits("field2")).containsExactly(new IntervalsQueryMatch.Hit(5, 26, 5, 30));

    }

    @Test
    public void testHighlighterQuery() throws IOException {

        InputDocument docWithMatch = buildDoc("1", "this is a test document");
        InputDocument docWithNoMatch = buildDoc("2", "this is a document");
        InputDocument docWithNoHighlighterMatch = buildDoc("3", "this is a test");

        monitor.update(new MonitorQuery("1", "test", "document"));

        IntervalsMatcher matcher1 = new IntervalsMatcher();
        monitor.match(docWithMatch, matcher1);
        assertThat(matcher1.getMatches().get(0).)

        assertThat(monitor.match(docWithMatch))
                .matchesQuery("1")
                .inField(textfield)
                .withHit(new IntervalsQueryMatch.Hit(4, 15, 4, 23));
        assertThat(monitor.match(docWithNoMatch))
                .doesNotMatchQuery("1");
        assertThat(monitor.match(docWithNoHighlighterMatch))
                .matchesQuery("1").inField(textfield)
                .withHit(new IntervalsQueryMatch.Hit(3, 10, 3, 14));

    }

    @Test
    public void testQueryErrors() {

        InputDocument doc = buildDoc("1", "this is a test document");
        monitor.update(new MonitorQuery("1", new TermQuery(new Term(textfield, "test"))),
                       new MonitorQuery("2", new MatchAllDocsQuery()),
                       new MonitorQuery("3", new TermQuery(new Term(textfield, "document"))),
                       new MonitorQuery("4", new TermQuery(new Term(textfield, "foo"))));

        assertThat(monitor.match(doc))
                .hasQueriesRunCount(4)
                .hasMatchCount(2)
                .hasErrorCount(1);

    }
}
