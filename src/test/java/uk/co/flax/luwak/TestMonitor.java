package uk.co.flax.luwak;

import com.google.common.collect.ImmutableList;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.Version;
import org.fest.assertions.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import uk.co.flax.luwak.impl.MatchAllPresearcher;

import java.util.List;

import static org.fest.assertions.api.Assertions.fail;
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

    static final String textfield = "textfield";

    private final Monitor monitor = new Monitor(new MatchAllPresearcher());

    @Before
    public void setUp() {
        monitor.reset();
    }

    @Test
    public void singleTermQueryMatchesSingleDocument() {

        String document = "This is a test document";

        Query query = new TermQuery(new Term(textfield, "test"));
        MonitorQuery mq = new MonitorQuery("query1", query);

        InputDocument doc = InputDocument.builder("doc1")
                .addField(textfield, document, WHITESPACE)
                .build();

        monitor.update(mq);

        assertThat(monitor.match(doc))
                .matches("doc1")
                .hasMatchCount(1)
                .matchesQuery("query1")
                    .withHitCount(1)
                    .inField(textfield)
                        .withHit(new QueryMatch.Hit(3, 10, 3, 14));

    }

    @Test(expected = IllegalStateException.class)
    public void monitorWithNoQueriesThrowsException() {
        InputDocument doc = InputDocument.builder("doc1").build();
        monitor.match(doc);
        fail("Monitor with no queries should have thrown an IllegalStateException");
    }

    static final Analyzer WHITESPACE = new WhitespaceAnalyzer(Version.LUCENE_50);

    @Test
    public void multiFieldQueryMatches() {

        InputDocument doc = InputDocument.builder("doc1")
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
                        .withHit(new QueryMatch.Hit(3, 10, 3, 14))
                    .inField("field2")
                        .withHit(new QueryMatch.Hit(5, 26, 5, 30));

    }

    public static InputDocument buildDoc(String id, String text) {
        return InputDocument.builder(id)
                .addField(textfield, text, WHITESPACE)
                .build();
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
                        .withHit(new QueryMatch.Hit(4, 15, 4, 23));
        assertThat(monitor.match(docWithNoMatch))
                .doesNotMatchQuery("1");
        assertThat(monitor.match(docWithNoHighlighterMatch))
                .matchesQuery("1").inField(textfield)
                    .withHit(new QueryMatch.Hit(3, 10, 3, 14));

    }

    @Test
    public void canAddMonitorQuerySubclasses() {

        class TestQuery extends MonitorQuery {
            public TestQuery(String id, String query) {
                super(id, new TermQuery(new Term("field", query)));
            }
        };

        List<TestQuery> queries = ImmutableList.of(new TestQuery("1", "foo"),
                                                   new TestQuery("2", "bar"));

        monitor.update(queries);

        Assertions.assertThat(monitor.getQuery("1"))
                .isNotNull()
                .isInstanceOf(TestQuery.class);

    }

}
