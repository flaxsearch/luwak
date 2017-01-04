package uk.co.flax.luwak;

import java.io.IOException;
import java.util.SortedMap;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import uk.co.flax.luwak.presearcher.MatchAllPresearcher;
import uk.co.flax.luwak.queryparsers.LuceneQueryParser;

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

public class TestDebugMonitor {

    static final String TEXTFIELD = "TEXTFIELD";

    static final Analyzer ANALYZER = new WhitespaceAnalyzer();

    private DebugMonitor monitor;

    @Before
    public void setUp() throws IOException {
        monitor = new DebugMonitor(new LuceneQueryParser(TEXTFIELD, ANALYZER), new MatchAllPresearcher());
    }

    @Test
    public void testEmptyDebugInfo() throws IOException {
        String msg = monitor.debugSubscription("my-id");
        Assertions.assertThat(msg).isEqualTo("");
    }

    @Test
    public void testNonEmptyDebugInfo() throws IOException, UpdateException {
        monitor.update(new MonitorQuery("my-id", "test"));
        monitor.update(new MonitorQuery("my-id2", "test2"));

        String msg = monitor.debugSubscription("my-id");
        Assertions.assertThat(msg).isEqualTo("doc: 0\n" +
                "hash:[9 8f 6b cd 46 21 d3 73 ca de 4e 83 26 27 b4 f6 5f 30]\n" +
                "cache entry found: {TEXTFIELD:test, {}}\n" +
                "fields:\n");

        msg = monitor.debugSubscription("my-id2");
        Assertions.assertThat(msg).isEqualTo("doc: 0\n" +
                "hash:[ad 2 34 82 92 5 b9 3 31 96 ba 81 8f 7a 87 2b 5f 30]\n" +
                "cache entry found: {TEXTFIELD:test2, {}}\n" +
                "fields:\n");
    }

    @Test
    public void testChecksums() throws IOException, UpdateException {
        monitor.update(new MonitorQuery("my-id", "test"));
        monitor.update(new MonitorQuery("my-id2", "test2"));
        monitor.update(new MonitorQuery("my-id3", "test"));

        SortedMap<String, String> checksums = monitor.indexChecksums();
        Assertions.assertThat(checksums.size()).isEqualTo(3);
        Assertions.assertThat(checksums.get("my-id")).isEqualTo("A544AE6535E4F690B52D3DBF5C2AB8BB");
        Assertions.assertThat(checksums.get("my-id2")).isEqualTo("8E301F36011347B1C1521C265B665032");
        Assertions.assertThat(checksums.get("my-id3")).isEqualTo("A544AE6535E4F690B52D3DBF5C2AB8BB");
    }
}
