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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import uk.co.flax.luwak.Matches;
import uk.co.flax.luwak.QueryMatch;

public class BenchmarkResults<T extends QueryMatch> {

    private final MetricRegistry metrics = new MetricRegistry();
    private final Timer timer = metrics.timer("searchTimes");
    private final Histogram queryBuildTimes = metrics.histogram("queryBuildTimes");

    public void add(Matches<T> benchmarkMatches) {
        timer.update(benchmarkMatches.getSearchTime(), TimeUnit.MILLISECONDS);
        queryBuildTimes.update(benchmarkMatches.getQueryBuildTime());
    }

    public Timer getTimer() {
        return timer;
    }

    @Override
    public String toString() {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try {
            PrintStream out = new PrintStream(os, true, StandardCharsets.UTF_8.name());
            ConsoleReporter.forRegistry(metrics).outputTo(out).build().report();
            return os.toString(StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
}
