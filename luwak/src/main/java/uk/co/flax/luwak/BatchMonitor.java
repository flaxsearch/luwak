package uk.co.flax.luwak;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * A Monitor contains a set of MonitorQuery objects, and runs them against
 * passed-in InputDocuments.
 */
public class BatchMonitor {

    private Monitor monitor;
    private long commitTimeout;
    private long lastCommitTime = 0;
    private volatile ScheduledFuture<?> commitJob = null;
    private final ScheduledExecutorService commitExecutor;

    private Map<String, Indexable> pendingUpdates = new ConcurrentHashMap<>();
    private Queue<String> pendingDeletes = new ConcurrentLinkedQueue<>();

    /**
     * Create a new BatchMonitor instance, that writes to the supplied monitor
     *
     * If set to non-zero, then the commit is delayed by up to that many milliseconds.
     * This means that multiple updates might be committed together.
     *
     * @param monitor the underlying monitor to write to
     * @param commitWithin the number of milliseconds to wait before committing any updates to this monitor.
     * @throws IOException on IO errors
     */
    public BatchMonitor(Monitor monitor, long commitWithin) throws IOException {
        this.monitor = monitor;
        this.commitTimeout = commitWithin;
        this.commitExecutor = Executors.newSingleThreadScheduledExecutor();
    }

    /**
     * Set the commit timeout in milliseconds.
     *
     * @param timeout time in milliseconds after which a record will be committed.
     */
    public void setCommitTimeout(long timeout) {
        this.commitTimeout = timeout;
    }

    private void scheduleCommit() {
        // If we are not bunching up commits, then this is a no-op.
        if (commitTimeout == 0) {
            return;
        }

        // If we have a pending commit job, then we are done,
        // that pending commit will pick up the changes
        if (commitJob != null && !commitJob.isDone()) {
            return;
        }

        // It maybe that lastCommitTime + commitTimeout - System.currentTimeMillis()
        // is negative, which is fine, the job will just run ASAP then.
        if (commitTimeout > 0) {
            commitJob = this.commitExecutor.schedule(new Runnable() {
                @Override
                public void run() {
                    try {
                        {
                            String[] deletes = pendingDeletes.toArray(new String[0]);
                            if (pendingDeletes.removeAll(Arrays.asList(deletes))) {
                                monitor.deleteById(deletes);
                            }
                        }
                        {
                            String[] keys = pendingUpdates.keySet().toArray(new String[0]);
                            Indexable[] updates = pendingUpdates.values().toArray(new Indexable[0]);
                            if (pendingUpdates.keySet().removeAll(Arrays.asList(keys))) {
                                monitor.commit(Arrays.asList(updates));
                            }
                        }
                        lastCommitTime = System.currentTimeMillis();
                        commitJob = null;
                    }
                    catch (Exception e) {
                        // TODO: How to deal with exceptions here?
                    }
                }
            }, lastCommitTime + commitTimeout - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Add new queries to the monitor
     * @param queries the MonitorQueries to add
     * @throws IOException on IO errors
     * @throws UpdateException if any of the queries could not be added
     */
    public void update(Iterable<MonitorQuery> queries) throws IOException, UpdateException {
        if (commitTimeout == 0) {
            monitor.update(queries);
        } else {
            List<QueryError> errors = new ArrayList<>();

            for (MonitorQuery query : queries) {
                try {
                    for (QueryCacheEntry queryCacheEntry : monitor.decomposeQuery(query)) {
                        // If we have a pending delete, then remove that
                        pendingDeletes.remove(query.getId());

                        // Create a pending update
                        Indexable update = new Indexable(query.getId(), queryCacheEntry,
                                monitor.buildIndexableQuery(query.getId(), query, queryCacheEntry));
                        pendingUpdates.put(update.id, update);
                    }
                } catch (Exception e) {
                    errors.add(new QueryError(query, e));
                }
            }

            scheduleCommit();

            if (errors.isEmpty() == false)
                throw new UpdateException(errors);
        }
    }

    /**
     * Add new queries to the monitor
     * @param queries the MonitorQueries to add
     * @throws IOException on IO errors
     * @throws UpdateException if any of the queries could not be added
     */
    public void update(MonitorQuery... queries) throws IOException, UpdateException {
        update(Arrays.asList(queries));
    }

    /**
     * Delete queries from the monitor
     * @param queries the queries to remove
     * @throws IOException on IO errors
     */
    public void delete(Iterable<MonitorQuery> queries) throws IOException {
        if (commitTimeout == 0) {
            monitor.delete(queries);
        } else {
            for (MonitorQuery mq : queries) {
                pendingUpdates.remove(mq.getId());
                pendingDeletes.offer(mq.getId());
            }

            scheduleCommit();
        }
    }

    /**
     * Delete queries from the monitor by ID
     * @param queryIds the IDs to delete
     * @throws IOException on IO errors
     */
    public void deleteById(Iterable<String> queryIds) throws IOException {
        if (commitTimeout == 0) {
            monitor.deleteById(queryIds);
        } else {
            for (String queryId : queryIds) {
                pendingUpdates.remove(queryId);
                pendingDeletes.offer(queryId);
            }

            scheduleCommit();
        }
    }

    /**
     * Delete queries from the monitor by ID
     * @param queryIds the IDs to delete
     * @throws IOException on IO errors
     */
    public void deleteById(String... queryIds) throws IOException {
        deleteById(Arrays.asList(queryIds));
    }

    /**
     * Delete all queries from the monitor
     * @throws IOException on IO errors
     */
    public void clear() throws IOException {
        monitor.clear();

        pendingUpdates.clear();
        pendingDeletes.clear();
    }
}
