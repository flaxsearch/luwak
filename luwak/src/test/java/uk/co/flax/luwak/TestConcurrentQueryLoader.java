package uk.co.flax.luwak;
/*
 *   Copyright (c) 2015 Lemur Consulting Ltd.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import uk.co.flax.luwak.presearcher.MatchAllPresearcher;
import uk.co.flax.luwak.queryparsers.LuceneQueryParser;

import static org.assertj.core.api.Assertions.assertThat;

public class TestConcurrentQueryLoader {

    @Test
    public void testLoading() throws Exception {

        try (Monitor monitor = new Monitor(new LuceneQueryParser("f"), new MatchAllPresearcher())) {
            List<QueryError> errors = new ArrayList<>();
            try (ConcurrentQueryLoader loader = new ConcurrentQueryLoader(monitor, errors)) {
                for (int i = 0; i < 2000; i++) {
                    loader.add(new MonitorQuery(Integer.toString(i), "\"test " + i + "\""));
                }
                assertThat(errors).isEmpty();
            }

            assertThat(monitor.getQueryCount()).isEqualTo(2000);

        }

    }

    @Test
    public void testErrorHandling() throws Exception {

        try (Monitor monitor = new Monitor(new LuceneQueryParser("f"), new MatchAllPresearcher())) {
            List<QueryError> errors = new ArrayList<>();
            try (ConcurrentQueryLoader loader = new ConcurrentQueryLoader(monitor, errors)) {
                for (int i = 0; i < 2000; i++) {
                    String query = "test" + i;
                    if (i % 200 == 0)
                        query += " [";
                    loader.add(new MonitorQuery(Integer.toString(i), query));
                }
            }

            assertThat(errors).hasSize(10);
            assertThat(monitor.getQueryCount()).isEqualTo(1990);

        }

    }
}
