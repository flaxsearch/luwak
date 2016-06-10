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
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Iterables;
import uk.co.flax.luwak.*;

public class Benchmark {

    private Benchmark() {}

    public static <T extends QueryMatch> BenchmarkResults<T> run(Monitor monitor, Iterable<InputDocument> documents,
                                                                 int batchsize, MatcherFactory<T> matcherFactory) throws IOException {
        BenchmarkResults<T> results = new BenchmarkResults<>();
        for (DocumentBatch batch : batchDocuments(documents, batchsize)) {
            Matches<T> matches = monitor.match(batch, matcherFactory);
            results.add(matches);
        }
        return results;
    }

    public static Iterable<DocumentBatch> batchDocuments(Iterable<InputDocument> documents, int batchsize) {
        Iterable<List<InputDocument>> partitions = Iterables.partition(documents, batchsize);
        final Iterator<List<InputDocument>> it = partitions.iterator();
        return new Iterable<DocumentBatch>() {
            @Override
            public Iterator<DocumentBatch> iterator() {
                return new Iterator<DocumentBatch>() {
                    @Override
                    public boolean hasNext() {
                        return it.hasNext();
                    }

                    @Override
                    public DocumentBatch next() {
                        return DocumentBatch.of(it.next());
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    public static BenchmarkResults<PresearcherMatch> timePresearcher(Monitor monitor, int batchsize, Iterable<InputDocument> documents)
            throws IOException {
        return run(monitor, documents, batchsize, PresearcherMatcher.FACTORY);
    }

    public static <T extends QueryMatch> ValidatorResults<T> validate(Monitor monitor, Iterable<ValidatorDocument<T>> documents,
                                                                      MatcherFactory<T> matcherFactory) throws IOException {
        ValidatorResults<T> results = new ValidatorResults<>();
        for (ValidatorDocument<T> doc : documents) {
            Matches<T> matches = monitor.match(doc.getDocument(), matcherFactory);
            results.add(matches, doc.getDocument().getId(), doc.getExpectedMatches());
        }
        return results;
    }

}
