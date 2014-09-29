package uk.co.flax.luwak;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Weight;
import org.junit.Test;
import uk.co.flax.luwak.matchers.SimpleMatcher;
import uk.co.flax.luwak.presearcher.MatchAllPresearcher;

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

public class TestSlowLog {

    static class SlowQueryParser implements MonitorQueryParser {

        final long delay;

        SlowQueryParser(long delay) {
            this.delay = delay;
        }

        @Override
        public Query parse(String queryString, Map<String, String> metadata) throws Exception {
            if (queryString.equals("slow")) {
                return new MatchAllDocsQuery() {
                    @Override
                    public Weight createWeight(IndexSearcher searcher) {
                        try {
                            Thread.sleep(delay);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        return super.createWeight(searcher);
                    }
                };
            }
            return new MatchAllDocsQuery();
        }
    }

    @Test
    public void testSlowLog() throws IOException {

        Monitor monitor = new Monitor(new SlowQueryParser(250), new MatchAllPresearcher());
        monitor.update(new MonitorQuery("1", "slow"), new MonitorQuery("2", "fast"), new MonitorQuery("3", "slow"));

        InputDocument doc1 = InputDocument.builder("doc1").build();

        SimpleMatcher matches = monitor.match(doc1, SimpleMatcher.FACTORY);
        assertThat(matches.getSlowLog())
                .contains("1:")
                .contains("3:")
                .doesNotContain("2:");

        monitor.setSlowLogLimit(1);
        assertThat(monitor.match(doc1, SimpleMatcher.FACTORY).getSlowLog())
                .contains("1:")
                .contains("2:")
                .contains("3:");

        monitor.setSlowLogLimit(2000000000000l);
        assertThat(monitor.match(doc1, SimpleMatcher.FACTORY).getSlowLog())
                .isEmpty();

    }

}
