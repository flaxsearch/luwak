package uk.co.flax.luwak;

import com.google.common.collect.Lists;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.lucene.search.Query;

public class MultithreadMonitorQueryCollector extends MonitorQueryCollector {
    private final int PACKAGE_SIZE = 1000;

    private ExecutorService executorService;

    private final QueryCache queryCache;
    private final CandidateMatcher matcher;
    private final List<QueryCollect> queries;
    private final List<Future<Object>> future = new LinkedList<>();

    private MultithreadMonitorQueryCollector(QueryCache queryCache, CandidateMatcher matcher) {
        this.queryCache = queryCache;
        this.matcher = matcher;
        this.queries = new LinkedList<>();
    }

    private class QueryCollect {

        public String queryId;
        Query matchQuery;
        Query highlightQuery;

        public QueryCollect(String queryId, Query matchQuery, Query highlightQuery) {
            this.queryId = queryId;
            this.matchQuery = matchQuery;
            this.highlightQuery = highlightQuery;
        }
    }

    private class MatchTask implements Callable<Object> {

        private CandidateMatcher matcher;
        private List<QueryCollect> queries;

        public MatchTask(CandidateMatcher matcher, List<QueryCollect> queries) {
            this.matcher = matcher;
            this.queries = queries;
        }

        @Override
        public Object call() throws Exception {
            for (QueryCollect query : queries) {
                matcher.matchQuery(query.queryId, query.matchQuery, query.highlightQuery);
            }
            return null;
        }
    }

    @Override
    protected void doSearch(MonitorQuery mq) {
        try {
            QueryCacheEntry query = queryCache.get(mq);
            synchronized (queries) {
                queries.add(new QueryCollect(mq.getId(), query.getQuery(), query.getHighlightQuery()));
            }
        } catch (Exception e) {
            matcher.reportError(new MatchError(mq.getId(), e));
        }
    }

    @Override
    protected void finish() {
        try {
            List<List<QueryCollect>> queriesLists = Lists.partition(queries, PACKAGE_SIZE);
            List<MatchTask> tasks = new LinkedList<>();
            for (List<QueryCollect> queriesList : queriesLists) {
                tasks.add(new MatchTask(matcher, queriesList));
            }
            executorService = Executors.newFixedThreadPool(queriesLists.size());
            future.addAll(executorService.invokeAll(tasks));
            for (Future<Object> task : future) {
                try {
                    task.get();
                } catch (ExecutionException | InterruptedException ex) {
                    matcher.reportError(new MatchError("", ex));
                }
            }
            executorService.shutdown();
        } catch (InterruptedException ex) {
            matcher.reportError(new MatchError("", ex));
        }
    }

    @Override
    public void setSearchTime(long searchTime) {
        matcher.setSearchTime(searchTime);
    }

    public static final MonitorQueryCollectorFactory FACTORY = new MonitorQueryCollectorFactory() {

        @Override
        public MonitorQueryCollector get(QueryCache queryCache, CandidateMatcher matcher) {
            return new MultithreadMonitorQueryCollector(queryCache, matcher);
        }

    };
}
