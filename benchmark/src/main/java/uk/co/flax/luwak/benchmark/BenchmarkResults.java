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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import uk.co.flax.luwak.Matches;
import uk.co.flax.luwak.QueryMatch;

public class BenchmarkResults<T extends QueryMatch> {

    private final Timer timer = new Timer();

    public void add(Matches<T> benchmarkMatches) {
        timer.update(benchmarkMatches.getSearchTime(), TimeUnit.MILLISECONDS);
    }

    public Timer getTimer() {
        return timer;
    }

    @Override
    public String toString() {
        StringWriter sw = new StringWriter();
        Locale locale = Locale.ROOT;
        try (PrintWriter output = new PrintWriter(sw)) {
            final Snapshot snapshot = timer.getSnapshot();
            output.printf(locale, "Benchmark Results%n");
            output.printf(locale, "             count = %d%n", timer.getCount());
            output.printf(locale, "         mean rate = %2.2f calls/%s%n", timer.getMeanRate(), "s");
            output.printf(locale, "     1-minute rate = %2.2f calls/%s%n", timer.getOneMinuteRate(), "s");
            output.printf(locale, "     5-minute rate = %2.2f calls/%s%n", timer.getFiveMinuteRate(), "s");
            output.printf(locale, "    15-minute rate = %2.2f calls/%s%n", timer.getFifteenMinuteRate(), "s");

            output.printf(locale, "               min = %d %s%n", TimeUnit.NANOSECONDS.toMillis(snapshot.getMin()), "ms");
            output.printf(locale, "               max = %d %s%n", TimeUnit.NANOSECONDS.toMillis(snapshot.getMax()), "ms");
            output.printf(locale, "              mean = %2.2f %s%n", snapshot.getMean() / 1000000, "ms");
            output.printf(locale, "            stddev = %2.2f %s%n", snapshot.getStdDev() / 1000000, "ms");
            output.printf(locale, "            median = %2.2f %s%n", snapshot.getMedian() / 1000000, "ms");
            output.printf(locale, "              75%% <= %2.2f %s%n", snapshot.get75thPercentile() / 1000000, "ms");
            output.printf(locale, "              95%% <= %2.2f %s%n", snapshot.get95thPercentile() / 1000000, "ms");
            output.printf(locale, "              98%% <= %2.2f %s%n", snapshot.get98thPercentile() / 1000000, "ms");
            output.printf(locale, "              99%% <= %2.2f %s%n", snapshot.get99thPercentile() / 1000000, "ms");
            output.printf(locale, "            99.9%% <= %2.2f %s%n", snapshot.get999thPercentile() / 1000000, "ms");

        }
        return sw.toString();
    }
}
