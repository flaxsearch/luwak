package uk.co.flax.luwak;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.Version;
import org.junit.Test;
import uk.co.flax.luwak.impl.MatchAllPresearcher;

import java.util.List;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

public class TestMonitorErrorHandling {

    public static final String FIELD = "f";

    public static final Analyzer ANALYZER = new WhitespaceAnalyzer(Version.LUCENE_50);

    private static MonitorQueryParser createMockParser() throws Exception {

        MonitorQueryParser parser = mock(MonitorQueryParser.class);
        Query errorQuery = mock(Query.class);
        when(errorQuery.rewrite(any(IndexReader.class))).thenThrow(new RuntimeException("Error rewriting"));

        when(parser.parse("unparseable")).thenThrow(new MonitorQueryParserException("Error parsing"));
        when(parser.parse("test")).thenReturn(new TermQuery(new Term(FIELD, "test")));
        when(parser.parse("error")).thenReturn(errorQuery);

        return parser;
    }

    @Test
    public void testMonitorErrors() throws Exception {

        Monitor monitor = new Monitor(createMockParser(), new MatchAllPresearcher());
        List<MonitorQueryParserException> errors = monitor.update(
                new MonitorQuery("1", "unparseable"),
                new MonitorQuery("2", "test"),
                new MonitorQuery("3", "error"));

        assertThat(errors).hasSize(1);

        InputDocument doc = InputDocument.builder("doc").addField(FIELD, "test", ANALYZER).build();
        SimpleMatcher matcher = monitor.match(doc);

        assertThat(matcher.getErrors()).hasSize(1);
        assertThat(matcher.getMatchCount()).isEqualTo(1);
        assertThat(matcher.getQueriesRun()).isEqualTo(2);
    }

}
