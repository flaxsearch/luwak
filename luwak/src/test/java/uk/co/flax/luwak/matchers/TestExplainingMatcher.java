package uk.co.flax.luwak.matchers;

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

import java.io.IOException;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.Test;
import uk.co.flax.luwak.InputDocument;
import uk.co.flax.luwak.Matches;
import uk.co.flax.luwak.Monitor;
import uk.co.flax.luwak.MonitorQuery;
import uk.co.flax.luwak.presearcher.MatchAllPresearcher;
import uk.co.flax.luwak.queryparsers.LuceneQueryParser;

import static org.assertj.core.api.Assertions.assertThat;

public class TestExplainingMatcher {

    @Test
    public void testExplainingMatcher() throws IOException {

        try (Monitor monitor = new Monitor(new LuceneQueryParser("field"), new MatchAllPresearcher())) {
            monitor.update(new MonitorQuery("1", "test"), new MonitorQuery("2", "wibble"));

            InputDocument doc1 = InputDocument.builder("doc1").addField("field", "test", new StandardAnalyzer()).build();

            Matches<ExplainingMatch> matches = monitor.match(doc1, ExplainingMatcher.FACTORY);
            assertThat(matches.matches("1", "doc1")).isNotNull();
            assertThat(matches.matches("1", "doc1").getExplanation()).isNotNull();
        }
    }
}
