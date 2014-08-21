package uk.co.flax.luwak.presearcher;

import java.io.IOException;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.junit.Test;
import uk.co.flax.luwak.InputDocument;
import uk.co.flax.luwak.Monitor;
import uk.co.flax.luwak.MonitorQuery;
import uk.co.flax.luwak.parsers.LuceneQueryCache;

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

public class TestPresearcherMatchCollector {

    public static final String TEXTFIELD = "f";

    @Test
    public void testMatchCollectorShowMatches() throws IOException {

        Monitor monitor = new Monitor(new LuceneQueryCache(TEXTFIELD), new TermFilteredPresearcher());
        monitor.update(new MonitorQuery("1", "test"));
        monitor.update(new MonitorQuery("2", "foo bar -baz"));

        InputDocument doc = InputDocument.builder("doc1")
                .addField(TEXTFIELD, "this is a foo test", new WhitespaceAnalyzer())
                .build();

        PresearcherMatchCollector collector = new PresearcherMatchCollector();
        monitor.match(doc, collector);

        assertThat(collector.matchingTerms)
                .containsKey("1")
                .containsKey("2");

        assertThat(collector.matchingTerms.get("1").toString()).isEqualTo(" f:test");
        assertThat(collector.matchingTerms.get("2").toString()).isEqualTo(" f:foo");

    }

}
