package uk.co.flax.luwak.presearcher;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import uk.co.flax.luwak.*;
import uk.co.flax.luwak.matchers.SimpleMatcher;
import uk.co.flax.luwak.termextractor.querytree.QueryTree;

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

        Map<String, StringBuilder> extractedTerms = presearcher.collectTerms(qt);

        Assertions.assertThat(extractedTerms.size()).isEqualTo(1);

    }
}
