package uk.co.flax.luwak.presearcher;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.analysis.NumericTokenStream;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.NumericUtils;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import uk.co.flax.luwak.*;
import uk.co.flax.luwak.matchers.SimpleMatcher;
import uk.co.flax.luwak.termextractor.querytree.QueryTree;

import static uk.co.flax.luwak.util.MatchesAssert.assertThat;

/*
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

public class TestTermPresearcher extends PresearcherTestBase {

    @Test
    public void filtersOnTermQueries() throws IOException {

        MonitorQuery query1
                = new MonitorQuery("1", "furble");
        MonitorQuery query2
                = new MonitorQuery("2", "document");
        monitor.update(query1, query2);

        InputDocument doc = InputDocument.builder("doc1")
                .addField(TEXTFIELD, "this is a test document", WHITESPACE)
                .build();

        Matches<QueryMatch> matcher = monitor.match(doc, SimpleMatcher.FACTORY);
        assertThat(matcher)
                .hasMatchCount(1)
                .hasQueriesRunCount(1);

    }

    @Test
    public void ignoresTermsOnNotQueries() throws IOException {

        monitor.update(new MonitorQuery("1", "document -test"));

        InputDocument doc1 = InputDocument.builder("doc1")
                .addField(TEXTFIELD, "this is a test document", WHITESPACE)
                .build();

        assertThat(monitor.match(doc1, SimpleMatcher.FACTORY))
                .hasMatchCount(0)
                .hasQueriesRunCount(1);

        InputDocument doc2 = InputDocument.builder("doc2")
                .addField(TEXTFIELD, "weeble sclup test", WHITESPACE)
                .build();

        assertThat(monitor.match(doc2, SimpleMatcher.FACTORY))
                .hasMatchCount(0)
                .hasQueriesRunCount(0);
    }

    @Test
    public void matchesAnyQueries() throws IOException {

        monitor.update(new MonitorQuery("1", "/hell./"));

        InputDocument doc = InputDocument.builder("doc1")
                .addField(TEXTFIELD, "hello", WHITESPACE)
                .build();

        assertThat(monitor.match(doc, SimpleMatcher.FACTORY))
                .hasMatchCount(1)
                .hasQueriesRunCount(1);

    }

    @Override
    protected Presearcher createPresearcher() {
        return new TermFilteredPresearcher();
    }

    @Test
    public void testAnyTermsAreCorrectlyAnalyzed() {

        TermFilteredPresearcher presearcher = new TermFilteredPresearcher();
        QueryTree qt = presearcher.extractor.buildTree(new MatchAllDocsQuery());

        Map<String, BytesRefHash> extractedTerms = presearcher.collectTerms(qt);

        Assertions.assertThat(extractedTerms.size()).isEqualTo(1);

    }

    @Test
    public void filtersOnNumericTermQueries() throws IOException {

        // Rudimentary query parser which returns numeric encoded BytesRefs
        try (Monitor numeric_monitor = new Monitor(new MonitorQueryParser() {
            @Override
            public Query parse(String queryString, Map<String, String> metadata) throws Exception
            {
                BytesRefBuilder brb = new BytesRefBuilder();
                NumericUtils.intToPrefixCoded(Integer.parseInt(queryString), 0, brb);

                Term t = new Term(TEXTFIELD, brb.get());
                return new TermQuery(t);
            }
        }, presearcher)) {

            MonitorQuery query8 = new MonitorQuery("query8", "8");
            MonitorQuery query9 = new MonitorQuery("query9", "9");
            MonitorQuery query10 = new MonitorQuery("query10", "10");
            MonitorQuery query11 = new MonitorQuery("query11", "11");
            MonitorQuery query12 = new MonitorQuery("query12", "12");
            MonitorQuery query13 = new MonitorQuery("query13", "13");
            MonitorQuery query14 = new MonitorQuery("query14", "14");
            MonitorQuery query15 = new MonitorQuery("query15", "15");
            numeric_monitor.update(query8, query9, query10, query11, query12, query13, query14, query15);

            NumericTokenStream nts8 = new NumericTokenStream(1);
            nts8.setIntValue(Integer.parseInt("8"));

            InputDocument doc8 = InputDocument.builder("doc8")
                    .addField(TEXTFIELD, nts8)
                    .build();

            assertThat(numeric_monitor.match(doc8, SimpleMatcher.FACTORY))
                    .matches("doc8")
                    .hasMatchCount(1)
                    .matchesQuery("query8")
                    .hasQueriesRunCount(1);

            NumericTokenStream nts9 = new NumericTokenStream(1);
            nts9.setIntValue(Integer.parseInt("9"));

            InputDocument doc9 = InputDocument.builder("doc9")
                    .addField(TEXTFIELD, nts9)
                    .build();

            assertThat(numeric_monitor.match(doc9, SimpleMatcher.FACTORY))
                    .matches("doc9")
                    .hasMatchCount(1)
                    .matchesQuery("query9")
                    .hasQueriesRunCount(1);

            NumericTokenStream nts10 = new NumericTokenStream(1);
            nts10.setIntValue(Integer.parseInt("10"));

            InputDocument doc10 = InputDocument.builder("doc10")
                    .addField(TEXTFIELD, nts10)
                    .build();

            assertThat(numeric_monitor.match(doc10, SimpleMatcher.FACTORY))
                    .matches("doc10")
                    .hasMatchCount(1)
                    .matchesQuery("query10")
                    .hasQueriesRunCount(1);

            NumericTokenStream nts11 = new NumericTokenStream(1);
            nts11.setIntValue(Integer.parseInt("11"));

            InputDocument doc11 = InputDocument.builder("doc11")
                    .addField(TEXTFIELD, nts11)
                    .build();

            assertThat(numeric_monitor.match(doc11, SimpleMatcher.FACTORY))
                    .matches("doc11")
                    .hasMatchCount(1)
                    .matchesQuery("query11")
                    .hasQueriesRunCount(1);

            NumericTokenStream nts12 = new NumericTokenStream(1);
            nts12.setIntValue(Integer.parseInt("12"));

            InputDocument doc12 = InputDocument.builder("doc12")
                    .addField(TEXTFIELD, nts12)
                    .build();

            assertThat(numeric_monitor.match(doc12, SimpleMatcher.FACTORY))
                    .matches("doc12")
                    .hasMatchCount(1)
                    .matchesQuery("query12")
                    .hasQueriesRunCount(1);

            NumericTokenStream nts13 = new NumericTokenStream(1);
            nts13.setIntValue(Integer.parseInt("13"));

            InputDocument doc13 = InputDocument.builder("doc13")
                    .addField(TEXTFIELD, nts13)
                    .build();

            assertThat(numeric_monitor.match(doc13, SimpleMatcher.FACTORY))
                    .matches("doc13")
                    .hasMatchCount(1)
                    .matchesQuery("query13")
                    .hasQueriesRunCount(1);

            NumericTokenStream nts14 = new NumericTokenStream(1);
            nts14.setIntValue(Integer.parseInt("14"));

            InputDocument doc14 = InputDocument.builder("doc14")
                    .addField(TEXTFIELD, nts14)
                    .build();

            assertThat(numeric_monitor.match(doc14, SimpleMatcher.FACTORY))
                    .matches("doc14")
                    .hasMatchCount(1)
                    .matchesQuery("query14")
                    .hasQueriesRunCount(1);

            NumericTokenStream nts15 = new NumericTokenStream(1);
            nts15.setIntValue(Integer.parseInt("15"));

            InputDocument doc15 = InputDocument.builder("doc15")
                    .addField(TEXTFIELD, nts15)
                    .build();

            assertThat(numeric_monitor.match(doc15, SimpleMatcher.FACTORY))
                    .matches("doc15")
                    .hasMatchCount(1)
                    .matchesQuery("query15")
                    .hasQueriesRunCount(1);
        }
    }
}
