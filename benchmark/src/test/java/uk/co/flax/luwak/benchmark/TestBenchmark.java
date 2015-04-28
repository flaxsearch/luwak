package uk.co.flax.luwak.benchmark;

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
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import uk.co.flax.luwak.InputDocument;
import uk.co.flax.luwak.Monitor;
import uk.co.flax.luwak.MonitorQuery;
import uk.co.flax.luwak.QueryMatch;
import uk.co.flax.luwak.matchers.SimpleMatcher;
import uk.co.flax.luwak.presearcher.TermFilteredPresearcher;
import uk.co.flax.luwak.queryparsers.LuceneQueryParser;

import static org.assertj.core.api.Assertions.assertThat;

public class TestBenchmark {

    private Monitor monitor;

    @Before
    public void startMonitor() throws IOException {
        monitor = new Monitor(new LuceneQueryParser("f"), new TermFilteredPresearcher());
        monitor.update(new MonitorQuery("1", "cheese"));
        monitor.update(new MonitorQuery("2", "sesquipedalian"));
        monitor.update(new MonitorQuery("3", "+goodbye +world"));
        monitor.update(new MonitorQuery("4", "text"));
    }

    @After
    public void stopMonitor() throws IOException {
        monitor.close();
    }

    private static final Analyzer STANDARD = new StandardAnalyzer();

    @Test
    public void testBasicBenchmarking() throws IOException {

        List<InputDocument> docs = ImmutableList.of(
                InputDocument.builder("doc1").addField("f", "some text about the world", STANDARD).build(),
                InputDocument.builder("doc2").addField("f", "some text about cheese", STANDARD).build()
        );

        BenchmarkResults<QueryMatch> results = Benchmark.run(monitor, docs, SimpleMatcher.FACTORY);

        assertThat(results.getTimer().getCount()).isEqualTo(2);

    }

    @Test
    public void testPresearcherBenchmarking() throws IOException {

        List<InputDocument> docs = ImmutableList.of(
                InputDocument.builder("doc1").addField("f", "some text about the world", STANDARD).build(),
                InputDocument.builder("doc2").addField("f", "some text about cheese", STANDARD).build()
        );

        BenchmarkResults<PresearcherMatch> results = Benchmark.timePresearcher(monitor, docs);

        assertThat(results.getTimer().getCount()).isEqualTo(2);
        assertThat(results.getTimer().getMeanRate()).isGreaterThan(0);

    }

    @Test
    public void testValidation() throws IOException {

        List<ValidatorDocument<QueryMatch>> docs = ImmutableList.of(
                vd("doc1", "some text about the world", new QueryMatch("3"), new QueryMatch("4")),  // 3: extra
                vd("doc2", "some text about cheese", new QueryMatch("1"), new QueryMatch("4")),     // accurate
                vd("doc3", "some text about cheese", new QueryMatch("1"))                           // 4: missing
        );

        ValidatorResults<QueryMatch> results = Benchmark.validate(monitor, docs, SimpleMatcher.FACTORY);

        assertThat(results.getTimer().getCount()).isEqualTo(3);
        assertThat(results.getCorrectMatchCount()).isEqualTo(1);
        assertThat(results.getBadDocuments()).containsOnly("doc1", "doc3");
        assertThat(results.getExtraMatches("doc1")).containsExactly(new QueryMatch("3"));
        assertThat(results.getMissingMatches("doc1")).isEmpty();
        assertThat(results.getMissingMatches("doc2")).isEmpty();
        assertThat(results.getExtraMatches("doc3")).isEmpty();
        assertThat(results.getMissingMatches("doc3")).containsExactly(new QueryMatch("4"));

    }

    private static ValidatorDocument<QueryMatch> vd(String id, String text, QueryMatch... expected) {
        return new ValidatorDocument<>(
                InputDocument.builder(id).addField("f", text, STANDARD).build(),
                Sets.newHashSet(expected)
        );
    }

}
