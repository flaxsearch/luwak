package uk.co.flax.luwak;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.junit.Test;
import uk.co.flax.luwak.matchers.SimpleMatcher;
import uk.co.flax.luwak.parsers.LuceneQueryCache;
import uk.co.flax.luwak.presearcher.MatchAllPresearcher;

import java.util.List;
import org.apache.lucene.util.BytesRef;

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

    public static final Analyzer ANALYZER = new WhitespaceAnalyzer(Constants.VERSION);

    private static QueryCache createMockCache() throws Exception {

        final Query errorQuery = mock(Query.class);
        when(errorQuery.rewrite(any(IndexReader.class))).thenThrow(new RuntimeException("Error rewriting"));

        return new ParsingQueryCache() {
            @Override
            protected Query parse(BytesRef query) throws Exception {
                if ("unparseable".equals(query.utf8ToString()))
                    throw new RuntimeException("Error parsing query [unparseable]");
                if ("error".equals(query.utf8ToString()))
                    return errorQuery;
                return new TermQuery(new Term(FIELD, query));
            }
        };

    }

    @Test
    public void testMonitorErrors() throws Exception {

        Monitor monitor = new Monitor(createMockCache(), new MatchAllPresearcher());
        List<QueryError> errors = monitor.update(
                new MonitorQuery("1", "unparseable"),
                new MonitorQuery("2", "test"),
                new MonitorQuery("3", "error"));

        assertThat(errors).hasSize(1);

        InputDocument doc = InputDocument.builder("doc").addField(FIELD, "test", ANALYZER).build();
        SimpleMatcher matcher = monitor.match(doc, SimpleMatcher.FACTORY);

        assertThat(matcher.getErrors()).hasSize(1);
        assertThat(matcher.getMatchCount()).isEqualTo(1);
        assertThat(matcher.getQueriesRun()).isEqualTo(2);
    }

    @Test
    public void testPresearcherErrors() throws Exception {

        Presearcher presearcher = mock(Presearcher.class);
        when(presearcher.indexQuery(any(Query.class)))
                .thenReturn(new Document())
                .thenThrow(new UnsupportedOperationException("Oops"))
                .thenReturn(new Document());

        Monitor monitor = new Monitor(new LuceneQueryCache("f"), presearcher);
        List<QueryError> errors
                = monitor.update(new MonitorQuery("1", "1"), new MonitorQuery("2", "2"), new MonitorQuery("3", "3"));

        assertThat(errors).hasSize(1);
        assertThat(errors.get(0).id).isEqualTo("2");
        assertThat(monitor.getQueryCount()).isEqualTo(2);

    }

}
