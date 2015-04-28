package uk.co.flax.luwak;

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

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.flax.luwak.matchers.SimpleMatcher;
import uk.co.flax.luwak.presearcher.MatchAllPresearcher;
import uk.co.flax.luwak.queryparsers.LuceneQueryParser;

import static org.assertj.core.api.Assertions.assertThat;

public class TestCachePurging {

    private static final Logger logger = LoggerFactory.getLogger(TestCachePurging.class);

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Test
    public void testQueryCacheCanBePurged() throws IOException {

        Monitor monitor = new Monitor(new LuceneQueryParser("field"), new MatchAllPresearcher());
        MonitorQuery[] queries = new MonitorQuery[] {
                new MonitorQuery("1", "test1 test4"),
                new MonitorQuery("2", "test2"),
                new MonitorQuery("3", "test3")
        };
        monitor.update(queries);
        assertThat(monitor.getQueryCount()).isEqualTo(3);
        assertThat(monitor.getDisjunctCount()).isEqualTo(4);
        assertThat(monitor.getStats().cachedQueries).isEqualTo(4);

        InputDocument doc = InputDocument.builder("doc1")
                .addField("field", "test1 test2 test3", new WhitespaceAnalyzer()).build();
        assertThat(monitor.match(doc, SimpleMatcher.FACTORY).getMatchCount()).isEqualTo(3);

        monitor.deleteById("1");
        assertThat(monitor.getQueryCount()).isEqualTo(2);
        assertThat(monitor.getStats().cachedQueries).isEqualTo(4);
        assertThat(monitor.match(doc, SimpleMatcher.FACTORY).getMatchCount()).isEqualTo(2);

        monitor.purgeCache();
        assertThat(monitor.getStats().cachedQueries).isEqualTo(2);

        Matches<QueryMatch> result = monitor.match(doc, SimpleMatcher.FACTORY);
        assertThat(result.getMatchCount()).isEqualTo(2);
    }

    @Test
    public void testConcurrentPurges() throws Exception {
        int iters = Integer.getInteger("purgeIters", 2);
        for (int i = 0; i < iters; i++) {
            doConcurrentPurgesAndUpdatesTest();
        }
    }

    private static void doConcurrentPurgesAndUpdatesTest() throws Exception {

        final CountDownLatch startUpdating = new CountDownLatch(1);
        final CountDownLatch finishUpdating = new CountDownLatch(1);

        final Monitor monitor = new Monitor(new LuceneQueryParser("field"), new MatchAllPresearcher());

        Runnable updaterThread = new Runnable() {
            @Override
            public void run() {
                try {
                    startUpdating.await();
                    for (int i = 200; i < 400; i++) {
                        logger.info("Updating with query {}", i);
                        monitor.update(newMonitorQuery(i));
                    }
                    finishUpdating.countDown();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };

        ExecutorService executor = Executors.newFixedThreadPool(1);
        try {
            executor.submit(updaterThread);

            for (int i = 0; i < 200; i++) {
                monitor.update(newMonitorQuery(i));
            }
            for (int i = 20; i < 80; i++) {
                monitor.deleteById(Integer.toString(i));
            }

            assertThat(monitor.getStats().cachedQueries).isEqualTo(200);

            logger.info("Starting cache purge");
            startUpdating.countDown();
            monitor.purgeCache();
            logger.info("Finished cache purge");
            finishUpdating.await();

            assertThat(monitor.getStats().cachedQueries).isEqualTo(340);
            InputDocument doc = InputDocument.builder("doc1")
                    .addField("field", "test", new WhitespaceAnalyzer()).build();
            Matches<QueryMatch> matcher = monitor.match(doc, SimpleMatcher.FACTORY);
            assertThat(matcher.getErrors()).isEmpty();
            assertThat(matcher.getMatchCount()).isEqualTo(340);


        }
        finally {
            executor.shutdownNow();
        }

    }

    private static MonitorQuery newMonitorQuery(int id) {
        return new MonitorQuery(Integer.toString(id), "+test " + Integer.toString(id));
    }

    @Test
    public void testBackgroundPurges() throws IOException, InterruptedException {

        Monitor monitor = new Monitor(new LuceneQueryParser("field"), new MatchAllPresearcher()) {
            @Override
            protected long configurePurgeFrequency() {
                return 2;
            }
        };

        assertThat(monitor.getStats().lastPurged).isEqualTo(-1);

        for (int i = 0; i < 100; i++) {
            monitor.update(newMonitorQuery(i));
        }
        monitor.deleteById("5");
        assertThat(monitor.getStats().queries).isEqualTo(99);
        assertThat(monitor.getStats().cachedQueries).isEqualTo(100);

        TimeUnit.SECONDS.sleep(3);
        assertThat(monitor.getStats().queries).isEqualTo(99);
        assertThat(monitor.getStats().cachedQueries).isEqualTo(99);
        assertThat(monitor.getStats().lastPurged).isGreaterThan(0);

    }

}
