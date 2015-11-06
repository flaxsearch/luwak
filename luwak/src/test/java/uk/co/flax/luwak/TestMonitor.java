package uk.co.flax.luwak;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.search.Query;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import uk.co.flax.luwak.matchers.SimpleMatcher;
import uk.co.flax.luwak.presearcher.MatchAllPresearcher;
import uk.co.flax.luwak.queryparsers.LuceneQueryParser;

import static uk.co.flax.luwak.assertions.MatchesAssert.assertThat;

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

    static final Analyzer ANALYZER = new WhitespaceAnalyzer();

    private Monitor monitor;

    @Before
    public void setUp() throws IOException {
        monitor = new Monitor(new LuceneQueryParser(TEXTFIELD, ANALYZER), new MatchAllPresearcher());
    }

    @Test
    public void singleTermQueryMatchesSingleDocument() throws IOException {

        String document = "This is a test document";

        DocumentBatch batch = DocumentBatch.of(InputDocument.builder("doc1")
                .addField(TEXTFIELD, document, ANALYZER)
                .build());

        monitor.update(new MonitorQuery("query1", "test"));

        assertThat(monitor.match(batch, SimpleMatcher.FACTORY))
                .matchesDoc("doc1")
                .hasMatchCount("doc1", 1)
                .matchesQuery("query1", "doc1");

    }

    @Test
    public void matchStatisticsAreReported() throws IOException {
        String document = "This is a test document";
        DocumentBatch batch = DocumentBatch.of(InputDocument.builder("doc1")
                .addField(TEXTFIELD, document, ANALYZER)
                .build());

        monitor.update(new MonitorQuery("query1", "test"));

        Matches<QueryMatch> matches = monitor.match(batch, SimpleMatcher.FACTORY);
        Assertions.assertThat(matches.getQueriesRun()).isEqualTo(1);
        Assertions.assertThat(matches.getQueryBuildTime()).isGreaterThan(-1);
        Assertions.assertThat(matches.getSearchTime()).isGreaterThan(-1);
    }

    @Test
    public void updatesOverwriteOldQueries() throws IOException {
        monitor.update(new MonitorQuery("query1", "this"));

        monitor.update(new MonitorQuery("query1", "that"));

        DocumentBatch batch = DocumentBatch.of(InputDocument.builder("doc1").addField(TEXTFIELD, "that", ANALYZER).build());
        assertThat(monitor.match(batch, SimpleMatcher.FACTORY))
                .hasQueriesRunCount(1)
                .matchesQuery("query1", "doc1");
    }

    @Test
    public void canDeleteById() throws IOException {

        Assertions.assertThat(monitor.update(new MonitorQuery("query1", "this"))).isEmpty();
        Assertions.assertThat(monitor.update(new MonitorQuery("query2", "that"), new MonitorQuery("query3", "other"))).isEmpty();
        Assertions.assertThat(monitor.getQueryCount()).isEqualTo(3);

        monitor.deleteById("query2", "query1");
        Assertions.assertThat(monitor.getQueryCount()).isEqualTo(1);

        DocumentBatch batch = DocumentBatch.of(InputDocument.builder("doc1").addField(TEXTFIELD, "other things", ANALYZER).build());
        assertThat(monitor.match(batch, SimpleMatcher.FACTORY))
                .hasQueriesRunCount(1)
                .matchesQuery("query3", "doc1");

    }

    @Test
    public void canRetrieveQuery() throws IOException {

        monitor.update(new MonitorQuery("query1", "this"), new MonitorQuery("query2", "that"));
        Assertions.assertThat(monitor.getQueryCount()).isEqualTo(2);
        Assertions.assertThat(monitor.getQueryIds()).contains("query1", "query2");

        MonitorQuery mq = monitor.getQuery("query2");
        Assertions.assertThat(mq).isEqualTo(new MonitorQuery("query2", "that"));

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

        InputDocument doc = InputDocument.builder("doc1").addField(TEXTFIELD, "other things", ANALYZER).build();
        Matches<QueryMatch> matches = monitor.match(doc, SimpleMatcher.FACTORY);

        Assertions.assertThat(matches.getQueriesRun()).isEqualTo(0);
    }

    @Test
    public void testUpdateReporting() throws IOException {

        List<MonitorQuery> queries = new ArrayList<>(10400);
        for (int i = 0; i < 10355; i++) {
            queries.add(new MonitorQuery(Integer.toString(i), "test"));
        }

        final int[] expectedSizes = new int[]{ 5001, 5001, 353 };
        final AtomicInteger callCount = new AtomicInteger();
        final AtomicInteger updateCount = new AtomicInteger();

        QueryIndexUpdateListener listener = new QueryIndexUpdateListener() {

            @Override
            public void afterUpdate(List<Monitor.Indexable> updates) {
                int calls = callCount.getAndIncrement();
                updateCount.addAndGet(updates.size());
                Assertions.assertThat(updates.size()).isEqualTo(expectedSizes[calls]);
            }
        };

        try (Monitor monitor = new Monitor(new LuceneQueryParser(TEXTFIELD, ANALYZER), new MatchAllPresearcher())) {
            monitor.addQueryIndexUpdateListener(listener);
            monitor.update(queries);
            Assertions.assertThat(updateCount.get()).isEqualTo(10355);
        }
    }

    @Test
    public void testMatcherMetadata() throws IOException {
        try (Monitor monitor = new Monitor(new LuceneQueryParser("field"), new MatchAllPresearcher())) {
            HashMap<String, String> metadataMap = new HashMap<>();
            metadataMap.put("key", "value");

            monitor.update(new MonitorQuery(Integer.toString(1), "+test " + Integer.toString(1), metadataMap));

            InputDocument doc = InputDocument.builder("1").addField("field", "test", ANALYZER).build();

            MatcherFactory<QueryMatch> testMatcherFactory = new MatcherFactory<QueryMatch>() {
                @Override
                public CandidateMatcher<QueryMatch> createMatcher(DocumentBatch docs) {
                    return new CandidateMatcher<QueryMatch>(docs) {
                        @Override
                        protected void doMatchQuery(String queryId, Query matchQuery, Map<String, String> metadata) throws IOException {
                            Assertions.assertThat(metadata.get("key")).isEqualTo("value");
                        }

                        @Override
                        public QueryMatch resolve(QueryMatch match1, QueryMatch match2) {
                            return null;
                        }
                    };
                }
            };

            monitor.match(doc, testMatcherFactory);
        }
    }

    @Test
    public void testDocumentBatching() throws IOException {

        DocumentBatch batch = DocumentBatch.of(
            InputDocument.builder("doc1").addField(TEXTFIELD, "this is a test", ANALYZER).build(),
            InputDocument.builder("doc2").addField(TEXTFIELD, "this is a kangaroo", ANALYZER).build()
        );

        monitor.clear();
        monitor.update(new MonitorQuery("1", "kangaroo"));

        Matches<QueryMatch> response = monitor.match(batch, SimpleMatcher.FACTORY);
        Assertions.assertThat(response.getBatchSize()).isEqualTo(2);
        Assertions.assertThat(response.iterator()).hasSize(2);

    }

}
