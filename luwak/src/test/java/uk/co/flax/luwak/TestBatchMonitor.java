package uk.co.flax.luwak;

import java.io.IOException;

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

public class TestBatchMonitor {

    static final String TEXTFIELD = "TEXTFIELD";

    static final Analyzer ANALYZER = new WhitespaceAnalyzer();

    private Monitor monitor;
    private BatchMonitor batch;

    @Before
    public void setUp() throws IOException {
        monitor = new Monitor(new LuceneQueryParser(TEXTFIELD, ANALYZER), new MatchAllPresearcher());
        batch = new BatchMonitor(monitor, 0);
    }

    @Test
    public void testMatcherNoCommitWithin() throws IOException, UpdateException {
        batch.setCommitTimeout(0);
        batch.update(new MonitorQuery("query1", "a"), new MonitorQuery("query2", "b"), new MonitorQuery("query3", "c"));

        // Should appear immediately
        Assertions.assertThat(monitor.getQueryCount()).isEqualTo(3);
    }

    @Test
    public void testMatcherCommitWithin() throws IOException, UpdateException {
        batch.setCommitTimeout(3000);
        batch.update(new MonitorQuery("query1", "a"), new MonitorQuery("query2", "b"), new MonitorQuery("query3", "c"));

        Assertions.assertThat(monitor.getQueryCount()).isEqualTo(0);

        try {
            Thread.sleep(3000);
        }
        catch (InterruptedException ex)
        {
            // TODO
        }

        Assertions.assertThat(monitor.getQueryCount()).isEqualTo(3);
    }

    @Test
    public void testMatcherAddDelete() throws IOException, UpdateException {
        batch.setCommitTimeout(3000);
        batch.update(new MonitorQuery("query1", "a"));
        batch.deleteById("query1");

        Assertions.assertThat(monitor.getQueryCount()).isEqualTo(0);

        try {
            Thread.sleep(3000);
        }
        catch (InterruptedException ex)
        {
            // TODO
        }

        Assertions.assertThat(monitor.getQueryCount()).isEqualTo(0);
    }

    @Test
    public void testMatcherSlowAddDelete() throws IOException, UpdateException {
        batch.setCommitTimeout(3000);
        batch.update(new MonitorQuery("query1", "a"));

        Assertions.assertThat(monitor.getQueryCount()).isEqualTo(0);

        try {
            Thread.sleep(3000);
        }
        catch (InterruptedException ex)
        {
            // TODO
        }

        Assertions.assertThat(monitor.getQueryCount()).isEqualTo(1);

        batch.deleteById("query1");

        Assertions.assertThat(monitor.getQueryCount()).isEqualTo(1);

        try {
            Thread.sleep(3000);
        }
        catch (InterruptedException ex)
        {
            // TODO
        }

        Assertions.assertThat(monitor.getQueryCount()).isEqualTo(0);
}
}
