package uk.co.flax.luwak.intervals;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Version;
import org.junit.Before;
import org.junit.Test;
import uk.co.flax.luwak.InputDocument;
import uk.co.flax.luwak.Monitor;
import uk.co.flax.luwak.MonitorQuery;
import uk.co.flax.luwak.impl.MatchAllPresearcher;

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

    static final Analyzer WHITESPACE = new WhitespaceAnalyzer(Version.LUCENE_50);

    private final Monitor monitor = new Monitor(new MatchAllPresearcher());

    public static InputDocument buildDoc(String id, String text) {
        return InputDocument.builder(id, QueryIntervalsMatchCollector.factory())
                .addField(textfield, text, WHITESPACE)
                .build();
    }

    @Before
    public void setUp() {
        monitor.reset();
    }

    @Test
    public void singleTermQueryMatchesSingleDocument() {

        Query query = new TermQuery(new Term(textfield, "test"));
        MonitorQuery mq = new MonitorQuery("query1", query);

        InputDocument doc = buildDoc("doc1", "This is a test document");

        monitor.update(mq);

        assertThat(monitor.match(doc))
                .matches("doc1")
                .hasMatchCount(1)
                .matchesQuery("query1")
                .withHitCount(1)
                .inField(textfield)
                .withHit(new IntervalsQueryMatch.Hit(3, 10, 3, 14));

    }

    @Test
    public void multiFieldQueryMatches() {

        InputDocument doc = InputDocument.builder("doc1", QueryIntervalsMatchCollector.factory())
                .addField("field1", "this is a test of field one", WHITESPACE)
                .addField("field2", "and this is an additional test", WHITESPACE)
                .build();

        BooleanQuery bq = new BooleanQuery();
        bq.add(new TermQuery(new Term("field1", "test")), BooleanClause.Occur.SHOULD);
        bq.add(new TermQuery(new Term("field2", "test")), BooleanClause.Occur.SHOULD);
        MonitorQuery mq = new MonitorQuery("query1", bq);

        monitor.update(mq);
        assertThat(monitor.match(doc))
                .matchesQuery("query1")
                .inField("field1")
                .withHit(new IntervalsQueryMatch.Hit(3, 10, 3, 14))
                .inField("field2")
                .withHit(new IntervalsQueryMatch.Hit(5, 26, 5, 30));

    }

    @Test
    public void testHighlighterQuery() {

        InputDocument docWithMatch = buildDoc("1", "this is a test document");
        InputDocument docWithNoMatch = buildDoc("2", "this is a document");
        InputDocument docWithNoHighlighterMatch = buildDoc("3", "this is a test");

        MonitorQuery mq = new MonitorQuery("1", new TermQuery(new Term(textfield, "test")),
                new TermQuery(new Term(textfield, "document")));

        monitor.update(mq);

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
