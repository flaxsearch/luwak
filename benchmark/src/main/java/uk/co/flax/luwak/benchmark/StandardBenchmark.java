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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import uk.co.flax.luwak.*;
import uk.co.flax.luwak.matchers.HighlightingMatcher;
import uk.co.flax.luwak.matchers.ScoringMatcher;
import uk.co.flax.luwak.matchers.SimpleMatcher;
import uk.co.flax.luwak.presearcher.MultipassTermFilteredPresearcher;
import uk.co.flax.luwak.presearcher.TermFilteredPresearcher;
import uk.co.flax.luwak.presearcher.WildcardNGramPresearcherComponent;
import uk.co.flax.luwak.queryparsers.LuceneQueryParser;

public class StandardBenchmark {

    public static final List<? extends Presearcher> PRESEARCHERS = ImmutableList.of(
            new TermFilteredPresearcher(),
            new MultipassTermFilteredPresearcher(2, 0.1f),
            new TermFilteredPresearcher(new WildcardNGramPresearcherComponent()),
            new MultipassTermFilteredPresearcher(2, 0.1f, new WildcardNGramPresearcherComponent())
    );

    public static final List<? extends MatcherFactory<? extends QueryMatch>> MATCHERS = ImmutableList.of(
            PresearcherMatcher.FACTORY,
            SimpleMatcher.FACTORY,
            ScoringMatcher.FACTORY,
            HighlightingMatcher.FACTORY
    );

    public static final String FIELD = "text";

    public static final Analyzer ANALYZER = new StandardAnalyzer();

    public static void main(String... args) throws IOException {

        for (Presearcher presearcher : PRESEARCHERS) {
            System.out.println("=================================================================");
            System.out.println("Benchmarking presearcher " + presearcher.toString());
            try (Monitor monitor = new Monitor(new LuceneQueryParser(FIELD), presearcher)) {
                long start = System.nanoTime();
                try {
                    monitor.update(loadQueries());
                } catch (UpdateException e) {
                    System.out.println(e.errors.size() + " queries had errors");
                }
                System.out.println("Loaded queries in " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) + " ms");
                // run through once to warm up
                Benchmark.run(monitor, loadDocuments(), 10, PresearcherMatcher.FACTORY);
                for (MatcherFactory<? extends QueryMatch> factory : MATCHERS) {
                    for (int batchSize : new int[]{ 1, 50, 100, 1000 }) {
                        System.out.println("Benchmarking presearcher " + presearcher.toString() + " with matcher " + factory.toString() + " and batchsize " + batchSize);
                        System.out.println(Benchmark.run(monitor, loadDocuments(), batchSize, factory));
                    }
                }
            }
        }

    }

    public static Iterable<MonitorQuery> loadQueries() throws IOException {
        List<MonitorQuery> queries = new ArrayList<>();
        int i = 0;
        for (String q : Resources.readLines(Resources.getResource("queries"), StandardCharsets.UTF_8)) {
            queries.add(new MonitorQuery(Integer.toString(i++), q));
        }
        return queries;
    }

    public static Iterable<InputDocument> loadDocuments() throws IOException {
        List<String> docfiles = Resources.readLines(Resources.getResource("doclist"), StandardCharsets.UTF_8);
        final Iterator<String> it = docfiles.iterator();
        return new Iterable<InputDocument>() {
            @Override
            public Iterator<InputDocument> iterator() {
                return new Iterator<InputDocument>() {
                    @Override
                    public boolean hasNext() {
                        return it.hasNext();
                    }

                    @Override
                    public InputDocument next() {
                        try {
                            String filename = "docfiles/" + it.next();
                            String input = Resources.toString(Resources.getResource(filename), StandardCharsets.UTF_8);
                            return InputDocument.builder(filename).addField(FIELD, input, ANALYZER).build();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    @Override
                    public void remove() {

                    }
                };
            }
        };
    }

}
