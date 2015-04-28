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

import uk.co.flax.luwak.*;

public class Benchmark {

    public static <T extends QueryMatch> BenchmarkResults<T> run(Monitor monitor, Iterable<InputDocument> documents,
                                                                 MatcherFactory<T> matcherFactory) throws IOException {
        BenchmarkResults<T> results = new BenchmarkResults<>();
        for (InputDocument doc : documents) {
            Matches<T> matches = monitor.match(doc, matcherFactory);
            results.add(matches);
        }
        return results;
    }

    public static BenchmarkResults<PresearcherMatch> timePresearcher(Monitor monitor, Iterable<InputDocument> documents)
            throws IOException {
        return run(monitor, documents, PresearcherMatcher.FACTORY);
    }

    public static <T extends QueryMatch> ValidatorResults<T> validate(Monitor monitor, Iterable<ValidatorDocument<T>> documents,
                                                                      MatcherFactory<T> matcherFactory) throws IOException {
        ValidatorResults<T> results = new ValidatorResults<>();
        for (ValidatorDocument doc : documents) {
            Matches<T> matches = monitor.match(doc.getDocument(), matcherFactory);
            results.add(matches, doc.getExpectedMatches());
        }
        return results;
    }
}
