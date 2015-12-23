package uk.co.flax.luwak;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LiveIndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

public class WriterAndCache {
    
    private final IndexWriter writer;
    private final SearcherManager manager;

    
    /* Used to cache updates while a purge is ongoing */
    private volatile Map<BytesRef, QueryCacheEntry> purgeCache = null;

    /* Used to lock around the creation of the purgeCache */
    private final ReadWriteLock purgeLock = new ReentrantReadWriteLock();
    private final Object commitLock = new Object();

    /* The current query cache */
    private volatile Map<BytesRef, QueryCacheEntry> queries = new ConcurrentHashMap<>();
    // NB this is not final because it can be replaced by purgeCache()
    
    public WriterAndCache(IndexWriter indexWriter, SearcherFactory searcherFactory) throws IOException {
        this.writer = indexWriter;

        this.manager = new SearcherManager(writer, true, searcherFactory);
    }
    
    public WriterAndCache() throws IOException {
        this(Monitor.defaultIndexWriter(new RAMDirectory()), new SearcherFactory());
    }

    public void commit(List<Indexable> updates, String deleteField) throws IOException {
        synchronized (commitLock) {
            purgeLock.readLock().lock();
            try {
                if (updates != null) {
                    Set<String> ids = new HashSet<>();
                    for (Indexable update : updates) {
                        ids.add(update.id);
                    }
                    for (String id : ids) {
                        writer.deleteDocuments(new Term(deleteField, id));
                    }
                    for (Indexable update : updates) {
                        this.queries.put(update.queryCacheEntry.hash, update.queryCacheEntry);
                        writer.addDocument(update.document);
                        if (purgeCache != null)
                            purgeCache.put(update.queryCacheEntry.hash, update.queryCacheEntry);
                    }
                }
                writer.commit();
                manager.maybeRefresh();
            } finally {
                purgeLock.readLock().unlock();
            }
        }
    }
    
    // Gets an IndexSearcher and sets the associated query cache on the passed-in collector
    // This is done within a readlock on the purge cache to ensure that a background purge
    // doesn't change the cache state getween the searcher being acquired and the map being set.
    public SearcherAndQueries getSearcher() throws IOException {
        try {
            purgeLock.readLock().lock();
            
            SearcherAndQueries saq = new SearcherAndQueries();
            
            saq.searcher = manager.acquire();
            saq.queries = queries;
            
            return saq;
        }
        finally {
            purgeLock.readLock().unlock();
        }
    }
    
    public interface CachePopulator {
        public void populateCacheWithIndex(ConcurrentMap<BytesRef, QueryCacheEntry> newCache) throws IOException;
    }
    
    /**
     * Remove unused queries from the query cache.
     *
     * This is normally called from a background thread at a rate set by configurePurgeFrequency().
     *
     * @throws IOException on IO errors
     */
    public synchronized void purgeCache(CachePopulator populator) throws IOException {

        /*
            Note on implementation

            The purge works by scanning the query index and creating a new query cache populated
            for each query in the index.  When the scan is complete, the old query cache is swapped
            for the new, allowing it to be garbage-collected.

            In order to not drop cached queries that have been added while a purge is ongoing,
            we use a ReadWriteLock to guard the creation and removal of an update log.  Commits take
            the read lock.  If the update log has been created, then a purge is ongoing, and queries
            are added to the update log within the read lock guard.

            The purge takes the write lock when creating the update log, and then when swapping out
            the old query cache.  Within the second write lock guard, the contents of the update log
            are added to the new query cache, and the update log itself is removed.
         */

        final ConcurrentMap<BytesRef, QueryCacheEntry> newCache = new ConcurrentHashMap<>();

        purgeLock.writeLock().lock();
        try {
            purgeCache = new ConcurrentHashMap<>();
        }
        finally {
            purgeLock.writeLock().unlock();
        }
        
        populator.populateCacheWithIndex(newCache);

        purgeLock.writeLock().lock();
        try {
            newCache.putAll(purgeCache);
            purgeCache = null;
            queries = newCache;
        }
        finally {
            purgeLock.writeLock().unlock();
        }
    }
    
    
    // ---------------------------------------------
    //  Proxy trivial atomic operations...
    // ---------------------------------------------

    public void closeWhileHandlingException() throws IOException {
        IOUtils.closeWhileHandlingException(manager, writer, writer.getDirectory());
    }

    public int numDocs() {
        return writer.numDocs();
    }

    public int numRamDocs() {
        return writer.numRamDocs();
    }

    public int cacheSize() {
        return queries.size();
    }

    public void deleteDocuments(Term term) throws IOException {
        writer.deleteDocuments(term);
    }

    public void deleteDocuments(Query query) throws IOException {
        writer.deleteDocuments(query);            
    }

    public void release(IndexSearcher searcher) throws IOException {
        manager.release(searcher);
    }

    /**
     * This should be used only at startup when no scans run.
     */
    public void addCacheEntry(BytesRef hash, QueryCacheEntry ce) {
        queries.put(hash, ce);            
    }
    
    public LiveIndexWriterConfig getConfig() {
        return this.writer.getConfig();
    }
    
    // ---------------------------------------------
    //  Helper classes...
    // ---------------------------------------------
    
    /**
     * An entry in the query cache
     */
    public static class QueryCacheEntry {

        /** The (possibly partial due to decomposition) query */
        public final Query matchQuery;

        /** A hash value for lookups */
        public final BytesRef hash;

        /** The metadata from the entry's parent {@link MonitorQuery} */
        public final Map<String,String> metadata;

        public QueryCacheEntry(BytesRef hash, Query matchQuery, Map<String, String> metadata) {
            this.hash = hash;
            this.matchQuery = matchQuery;
            this.metadata = metadata;
        }
    }
    
    public static class SearcherAndQueries {
        /**
         * Consider this field C++ const.
         */
        public Map<BytesRef, QueryCacheEntry> queries;
        public IndexSearcher searcher;
    }
}
