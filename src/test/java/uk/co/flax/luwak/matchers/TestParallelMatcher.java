package uk.co.flax.luwak.matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.junit.Test;
import uk.co.flax.luwak.*;
import uk.co.flax.luwak.presearcher.MatchAllPresearcher;
import uk.co.flax.luwak.queryparsers.LuceneQueryParser;

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

public class TestParallelMatcher {

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

        CandidateMatcher<QueryMatch> matches
                = monitor.match(doc, ParallelMatcher.factory(executor, SimpleMatcher.FACTORY, 10));

        assertThat(matches.getMatchCount()).isEqualTo(1000);

    }

}
