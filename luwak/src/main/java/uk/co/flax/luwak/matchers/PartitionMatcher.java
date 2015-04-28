package uk.co.flax.luwak.matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.lucene.search.Query;
import uk.co.flax.luwak.*;
import uk.co.flax.luwak.util.CollectionUtils;

/*
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

/**
 * A multi-threaded matcher that collects all possible matches in one pass, and
 * then partitions them amongst a number of worker threads to perform the actual
 * matching.
 *
 * This class delegates the matching to separate CandidateMatcher classes,
 * built from a passed in MatcherFactory.
 *
 * Use this if your query sets contain large numbers of very fast queries, where
 * the synchronization overhead of {@link uk.co.flax.luwak.matchers.ParallelMatcher}
 * can outweigh the benefit of multithreading.
 *
 * @see uk.co.flax.luwak.matchers.ParallelMatcher
 *
 * @param <T> the type of QueryMatch to return
 */
public class PartitionMatcher<T extends QueryMatch> extends CandidateMatcher<T> {

    private final ExecutorService executor;

    private final MatcherFactory<T> matcherFactory;

    private final int threads;

    private final CandidateMatcher<T> resolvingMatcher;

    private static class MatchTask {

        final String queryId;
        final Query matchQuery;
        final Query highlightQuery;

        private MatchTask(String queryId, Query matchQuery, Query highlightQuery) {
            this.queryId = queryId;
            this.matchQuery = matchQuery;
            this.highlightQuery = highlightQuery;
        }
    }

    private final List<MatchTask> tasks = new ArrayList<>();

    public PartitionMatcher(InputDocument doc, ExecutorService executor, MatcherFactory<T> matcherFactory, int threads) {
        super(doc);
        this.executor = executor;
        this.matcherFactory = matcherFactory;
        this.threads = threads;
        this.resolvingMatcher = matcherFactory.createMatcher(doc);
    }

    @Override
    public T matchQuery(String queryId, Query matchQuery, Query highlightQuery) throws IOException {
        tasks.add(new MatchTask(queryId, matchQuery, highlightQuery));
        return null;
    }

    @Override
    public T resolve(T match1, T match2) {
        return resolvingMatcher.resolve(match1, match2);
    }

    @Override
    public void finish(long buildTime, int queryCount) {

        List<Callable<Matches<T>>> workers = new ArrayList<>(threads);
        for (List<MatchTask> taskset : CollectionUtils.partition(tasks, threads)) {
            CandidateMatcher<T> matcher = matcherFactory.createMatcher(doc);
            matcher.setSlowLogLimit(this.slowLogLimit);
            workers.add(new MatcherWorker(taskset, matcher));
        }

        try {
            for (Future<Matches<T>> future : executor.invokeAll(workers)) {
                Matches<T> matches = future.get();
                for (T match : matches) {
                    addMatch(match.getQueryId(), match);
                }
                this.slowlog.append(matches.getSlowLog());
            }

        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Interrupted during match", e);
        }

        super.finish(buildTime, queryCount);
    }

    private class MatcherWorker implements Callable<Matches<T>> {

        final List<MatchTask> tasks;
        final CandidateMatcher<T> matcher;

        private MatcherWorker(List<MatchTask> tasks, CandidateMatcher<T> matcher) {
            this.tasks = tasks;
            this.matcher = matcher;
        }

        @Override
        public Matches<T> call() {
            for (MatchTask task : tasks) {
                try {
                    matcher.matchQuery(task.queryId, task.matchQuery, task.highlightQuery);
                } catch (IOException e) {
                    PartitionMatcher.this.reportError(new MatchError(task.queryId, e));
                }
            }
            return matcher.getMatches();
        }
    }

    public static class PartitionMatcherFactory<T extends QueryMatch> implements MatcherFactory<T> {

        private final ExecutorService executor;
        private final MatcherFactory<T> matcherFactory;
        private final int threads;

        public PartitionMatcherFactory(ExecutorService executor, MatcherFactory<T> matcherFactory,
                                      int threads) {
            this.executor = executor;
            this.matcherFactory = matcherFactory;
            this.threads = threads;
        }

        @Override
        public PartitionMatcher<T> createMatcher(InputDocument doc) {
            return new PartitionMatcher<>(doc, executor, matcherFactory, threads);
        }
    }

    /**
     * Create a new PartitionMatcherFactory
     * @param executor the ExecutorService to use
     * @param matcherFactory the MatcherFactory to use to create submatchers
     * @param threads the number of threads to use
     * @param <T> the type of QueryMatch generated
     * @return a PartitionMatcherFactory
     */
    public static <T extends QueryMatch> PartitionMatcherFactory<T> factory(ExecutorService executor,
                                                                           MatcherFactory<T> matcherFactory, int threads) {
        return new PartitionMatcherFactory<>(executor, matcherFactory, threads);
    }

    /**
     * Create a new PartitionMatcherFactory
     *
     * This factory will create a PartitionMatcher that uses as many threads as there are cores available
     * to the JVM (as determined by {@code Runtime.getRuntime().availableProcessors()}).
     *
     * @param executor the ExecutorService to use
     * @param matcherFactory the MatcherFactory to use to create submatchers
     * @param <T> the type of QueryMatch generated
     * @return a PartitionMatcherFactory
     */
    public static <T extends QueryMatch> PartitionMatcherFactory<T> factory(ExecutorService executor,
                                                                           MatcherFactory<T> matcherFactory) {
        int threads = Runtime.getRuntime().availableProcessors();
        return new PartitionMatcherFactory<>(executor, matcherFactory, threads);
    }
}
