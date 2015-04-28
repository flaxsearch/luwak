package uk.co.flax.luwak.matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.junit.Test;
import uk.co.flax.luwak.*;
import uk.co.flax.luwak.presearcher.MatchAllPresearcher;
import uk.co.flax.luwak.queryparsers.LuceneQueryParser;

import static org.assertj.core.api.Assertions.assertThat;

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

public abstract class ConcurrentMatcherTestBase {

    protected abstract <T extends QueryMatch>
        MatcherFactory<T> matcherFactory(ExecutorService executor, MatcherFactory<T> factory, int threads);

    @Test
    public void testAllMatchesAreCollected() throws IOException {

        Monitor monitor = new Monitor(new LuceneQueryParser("field"), new MatchAllPresearcher());
        List<MonitorQuery> queries = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            queries.add(new MonitorQuery(Integer.toString(i), "+test " + Integer.toString(i)));
        }
        monitor.update(queries);

        ExecutorService executor = Executors.newFixedThreadPool(10);

        InputDocument doc = InputDocument.builder("1").addField("field", "test", new KeywordAnalyzer()).build();

        Matches<QueryMatch> matches
                = monitor.match(doc, matcherFactory(executor, SimpleMatcher.FACTORY, 10));

        assertThat(matches.getMatchCount()).isEqualTo(1000);

    }

    @Test
    public void testMatchesAreDisambiguated() throws IOException {

        Monitor monitor = new Monitor(new LuceneQueryParser("field"), new MatchAllPresearcher());
        List<MonitorQuery> queries = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            queries.add(new MonitorQuery(Integer.toString(i), "test^10 doc " + Integer.toString(i)));
        }
        monitor.update(queries);
        assertThat(monitor.getDisjunctCount()).isEqualTo(30);

        ExecutorService executor = Executors.newFixedThreadPool(4);

        InputDocument doc = InputDocument.builder("1")
                .addField("field", "test doc doc", new WhitespaceAnalyzer())
                .build();

        Matches<ScoringMatch> matches
                = monitor.match(doc, matcherFactory(executor, ScoringMatcher.FACTORY, 10));

        assertThat(matches.getMatchCount()).isEqualTo(10);
        assertThat(matches.getQueriesRun()).isEqualTo(30);
        assertThat(matches.getErrors()).isEmpty();
        for (ScoringMatch match : matches) {
            // The queries are all split into three by the QueryDecomposer, and the
            // 'test' and 'doc' parts will match.  'test' will have a higher score,
            // because of it's lower termfreq.  We need to check that each query ends
            // up with the score for the 'test' subquery, not the 'doc' subquery
            assertThat(match.getScore()).isEqualTo(0.2169777f);
        }

    }

    @Test
    public void testParallelSlowLog() throws IOException {

        ExecutorService executor = Executors.newCachedThreadPool();

        Monitor monitor = new Monitor(new TestSlowLog.SlowQueryParser(250), new MatchAllPresearcher());
        monitor.update(new MonitorQuery("1", "slow"), new MonitorQuery("2", "fast"), new MonitorQuery("3", "slow"));

        InputDocument doc1 = InputDocument.builder("doc1").build();

        MatcherFactory<QueryMatch> factory
                = matcherFactory(executor, SimpleMatcher.FACTORY, 10);

        Matches<QueryMatch> matches = monitor.match(doc1, factory);
        assertThat(matches.getMatchCount())
                .isEqualTo(3);
        System.out.println(matches.getSlowLog());
        assertThat(matches.getSlowLog())
                .contains("1:")
                .contains("3:")
                .doesNotContain("2:");

        monitor.setSlowLogLimit(1);
        assertThat(monitor.match(doc1, factory).getSlowLog())
                .contains("1:")
                .contains("2:")
                .contains("3:");

        monitor.setSlowLogLimit(2000000000000l);
        assertThat(monitor.match(doc1, factory).getSlowLog())
                .isEmpty();

    }

}
