package uk.co.flax.luwak.matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import org.apache.lucene.search.Query;
import uk.co.flax.luwak.*;

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
 * distributed under the License is distributed on an "AS IS" BASIS
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Matcher class that runs matching queries in parallel.
 *
 * This class delegates the actual matching to separate CandidateMatcher classes,
 * built from a passed in MatcherFactory.
 *
 * @param <T> the QueryMatch type returned
 */
public class ParallelMatcher<T extends QueryMatch> extends CandidateMatcher<T> {

    private final BlockingQueue<MatcherTask> queue = new LinkedBlockingQueue<>(1024);

    private final List<Future<CandidateMatcher<T>>> futures = new ArrayList<>();

    /**
     * Create a new ParallelMatcher
     * @param doc the InputDocument to match against
     * @param executor an ExecutorService to use for parallel execution
     * @param matcherFactory MatcherFactory to use to create CandidateMatchers
     * @param threads the number of threads to execute on
     */
    public ParallelMatcher(InputDocument doc, ExecutorService executor,
                           MatcherFactory<? extends CandidateMatcher<T>> matcherFactory, int threads) {
        super(doc);
        for (int i = 0; i < threads; i++) {
            futures.add(executor.submit(new MatcherWorker(matcherFactory)));
        }
    }

    @Override
    public T matchQuery(String queryId, Query matchQuery, Query highlightQuery) throws IOException {
        try {
            queue.put(new MatcherTask(queryId, matchQuery, highlightQuery));
        } catch (InterruptedException e) {
            throw new IOException("Interrupted during match");
        }
        return null;
    }

    @Override
    public void finish(long buildTime, int queryCount) {
        super.finish(buildTime, queryCount);
        try {

            //noinspection ForLoopReplaceableByForEach
            for (int i = 0; i < futures.size(); i++) {
                queue.put(END);
            }

            for (Future<CandidateMatcher<T>> future : futures) {
                CandidateMatcher<T> matches = future.get();
                for (T match : matches) {
                    this.addMatch(match.getQueryId(), match);
                }
                for (MatchError error : matches.getErrors()) {
                    this.reportError(error);
                }
                this.slowlog.append(matches.getSlowLog());
            }

        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Interrupted during match");
        }
    }

    private class MatcherWorker implements Callable<CandidateMatcher<T>> {

        final CandidateMatcher<T> matcher;

        private MatcherWorker(MatcherFactory<? extends CandidateMatcher<T>> matcherFactory) {
            this.matcher = matcherFactory.createMatcher(doc);
            this.matcher.setSlowLogLimit(slowLogLimit);
        }

        @Override
        public CandidateMatcher<T> call() {
            MatcherTask task;
            try {
                while ((task = queue.take()) != END) {
                    try {
                        matcher.matchQuery(task.id, task.matchQuery, task.highlightQuery);
                    } catch (IOException e) {
                        matcher.reportError(new MatchError(task.id, e));
                    }
                }
            } catch (InterruptedException e) {
                throw new RuntimeException("Interrupted during match");
            }
            return matcher;
        }

    }

    private static class MatcherTask {

        final String id;
        final Query matchQuery;
        final Query highlightQuery;

        private MatcherTask(String id, Query matchQuery, Query highlightQuery) {
            this.id = id;
            this.matchQuery = matchQuery;
            this.highlightQuery = highlightQuery;
        }
    }

    /* Marker object placed on the queue after all matches are done, to indicate to the
       worker threads that they should finish */
    private static final MatcherTask END = new MatcherTask("", null, null);

    public static class ParallelMatcherFactory<T extends QueryMatch> implements MatcherFactory<ParallelMatcher<T>> {

        private final ExecutorService executor;
        private final MatcherFactory<? extends CandidateMatcher<T>> matcherFactory;
        private final int threads;

        public ParallelMatcherFactory(ExecutorService executor, MatcherFactory<? extends CandidateMatcher<T>> matcherFactory,
                                      int threads) {
            this.executor = executor;
            this.matcherFactory = matcherFactory;
            this.threads = threads;
        }

        @Override
        public ParallelMatcher<T> createMatcher(InputDocument doc) {
            return new ParallelMatcher<>(doc, executor, matcherFactory, threads);
        }
    }

    /**
     * Create a new ParallelMatcherFactory
     * @param executor the ExecutorService to use
     * @param matcherFactory the MatcherFactory to use to create submatchers
     * @param threads the number of threads to use
     * @param <T> the type of QueryMatch generated
     * @return a ParallelMatcherFactory
     */
    public static <T extends QueryMatch> ParallelMatcherFactory<T> factory(ExecutorService executor, MatcherFactory<? extends CandidateMatcher<T>> matcherFactory, int threads) {
        return new ParallelMatcherFactory<>(executor, matcherFactory, threads);
    }

    /**
     * Create a new ParallelMatcherFactory
     *
     * This factory will create a ParallelMatcher that uses as many threads as there are cores available
     * to the JVM (as determined by {@code Runtime.getRuntime().availableProcessors()}).
     *
     * @param executor the ExecutorService to use
     * @param matcherFactory the MatcherFactory to use to create submatchers
     * @param <T> the type of QueryMatch generated
     * @return a ParallelMatcherFactory
     */
    public static <T extends QueryMatch> ParallelMatcherFactory<T> factory(ExecutorService executor, MatcherFactory<? extends CandidateMatcher<T>> matcherFactory) {
        int threads = Runtime.getRuntime().availableProcessors();
        return new ParallelMatcherFactory<>(executor, matcherFactory, threads);
    }

}
