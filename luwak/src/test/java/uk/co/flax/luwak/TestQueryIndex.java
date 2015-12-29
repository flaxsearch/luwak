package uk.co.flax.luwak;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.lucene.document.*;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.flax.luwak.Monitor.FIELDS;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Run with {@code mvn test -Dtests.slow=true}
 */
public class TestQueryIndex {

    private final static double FREQUENCY_ADD_PERTHOUSAND = 485;
    private final static double FREQUENCY_DELETE_PERTHOUSAND = 485;
    private final static double FREQUENCY_PURGE_PERTHOUSAND = 30;
    
    private final static int ACTIVE_OPS_THREADS = 4;
    
    private final static int ACTIVE_QUERY_THREADS = 1;
    
    private final static int MAX_ID = 100;
    
    private static final Logger logger = LoggerFactory.getLogger(TestQueryIndex.class);
    
    private static final String SYSPROP_SLOW = "tests.slow";
    
    private volatile boolean running = true;
    
    private QueryIndex queryIndex;

    @Before
    public void setUp() throws IOException {
        Assume.assumeTrue(System.getProperty(SYSPROP_SLOW, "").equalsIgnoreCase("true"));
        queryIndex = new QueryIndex();
    }
    
    @Test
    public void testSmoke() throws Exception {

        assertThat(getSumOfIds()[0]).isEqualTo(0);

        queryIndex.commit(createUpdate(12));
        
        assertThat(getSumOfIds()[0]).isEqualTo(12);
        
        queryIndex.deleteDocuments(createDelete(12));
        
        assertThat(getSumOfIds()[0]).isEqualTo(12);
        
        queryIndex.commit(null);
        
        assertThat(getSumOfIds()[0]).isEqualTo(0);
        
    }
    
    /**
     * Tests adding/removing entries from multiple threads.
     * To run just this, {@code mvn test -Dtests.slow=true -Dtest=TestQueryIndex#testHighConcurrency}
     * @throws Exception
     */
    @Test
    public void testHighConcurrency() throws Exception {

        ExecutorService pool = Executors.newFixedThreadPool(ACTIVE_OPS_THREADS + ACTIVE_QUERY_THREADS);
        
        Random r = new Random();

        List<OpsThread> opsThreads = new ArrayList<>();
        for (int i = 0; i < ACTIVE_OPS_THREADS; i++) {
            OpsThread ot = new OpsThread(r.nextInt(1000));
            opsThreads.add(ot);
            pool.execute(ot);
        }

        List<QueryThread> queryThreads = new ArrayList<>();
        for (int i = 0; i < ACTIVE_QUERY_THREADS; i++) {
            QueryThread qt = new QueryThread();
            queryThreads.add(qt);
            pool.execute(qt);
        }
        
        Thread.sleep(15000);
        
        running = false;

        pool.shutdown();

        while (!pool.isTerminated()) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                // ok
            }
        }
        
        for (OpsThread o: opsThreads) {
            logger.info("thread/ adds={} deletes={} purges={}", o.adds, o.deletes, o.purges);
        }

        for (QueryThread o: queryThreads) {
            
            double avgValue = (((double)o.sum) / ((double)o.scans));
            double expectedAvgValue = (MAX_ID*MAX_ID)/4;
            
            double avgCount = (((double)o.cnt) / ((double)o.scans));
            double expectedAvgCount = MAX_ID/2;
            
            logger.info("thread/ scans={}", o.cnt);
            logger.info("average # of records={}, average value of records={} (should be a bit less than ({}*{})/2 = {})", (int)avgCount, (int)avgValue, MAX_ID, MAX_ID/2, expectedAvgValue);
            
            // If this fails try to run longer - it needs 15s. If it still fails - try to find out why. This should
            // have this value because reasonably the average ID values are MAX_ID/2. How many IDs we have at a given
            // time? Deletes are targeting random ids. In order to remove them they have to exist. This dynamic
            // of producers and consumers with equal strength (FREQUENCY_ADD_PERTHOUSAND == FREQUENCY_DELETE_PERTHOUSAND)
            // leads to 50% of the max being on average available.
            assertThat(avgValue).isCloseTo(expectedAvgValue, within((double)MAX_ID));
            assertThat(avgCount).isCloseTo(expectedAvgCount, within((double)MAX_ID/10));
        }
    }
    
    public class OpsThread implements Runnable {
        public volatile int adds = 0;
        public volatile int deletes = 0;
        public volatile int purges = 0;
        
        private final Random r;
        
        
        public OpsThread(long seed) {
            r = new Random(seed);
        }

        // Full time add/remove subscriptions
        @Override
        public void run() {
            double allFreqs = FREQUENCY_ADD_PERTHOUSAND + FREQUENCY_DELETE_PERTHOUSAND + FREQUENCY_PURGE_PERTHOUSAND;
            double normAdd = FREQUENCY_ADD_PERTHOUSAND / allFreqs;
            double normDelete = FREQUENCY_DELETE_PERTHOUSAND / allFreqs;
            
            while (running) {
                try {
                    double dice = r.nextDouble();
                    
                    dice -= normAdd;
                    if (dice < 0) {
                        // Add
                        add();
                        ++adds;
                        continue;
                    }
                    
                    dice -= normDelete;
                    if (dice < 0) {
                        // Delete
                        delete();
                        ++deletes;
                        continue;
                    }
                    
                    {
                        purgeCache();
                        ++purges;
                    }
                    
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        
        private void add() throws IOException {
            queryIndex.commit(createUpdate(r.nextInt(MAX_ID)));
        }
        
        private void delete() throws IOException {
            queryIndex.deleteDocuments(createDelete(r.nextInt(MAX_ID)));
        }
        
        private void purgeCache() throws IOException {
            
            queryIndex.purgeCache(new QueryIndex.CachePopulator() {
                
                @Override
                public void populateCacheWithIndex(final Map<BytesRef, QueryCacheEntry> newCache) throws IOException {
                    queryIndex.scan(new QueryIndex.QueryCollector() {
                        @Override
                        public void matchQuery(String id, QueryCacheEntry query, QueryIndex.DataValues dataValues) throws IOException {
                            newCache.put(BytesRef.deepCopyOf(query.hash), query);
                        }
                    });
                }
                
            });
        }
    }

    public class QueryThread implements Runnable {
        
        public volatile int sum = 0;
        public volatile int cnt = 0;
        public volatile int scans = 0;
        
        // Full time running scans
        @Override
        public void run() {
            while (running) {
                
                try {
                    
                    final int[] aggr = getSumOfIds();
                    
                    sum += aggr[0];
                    cnt += aggr[1];
                    
                    ++scans;
                    
                } catch (IOException e) {
                    e.printStackTrace();
                }
                
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    Thread.interrupted();
                    return;
                }
            }
        }
    }

    private int[] getSumOfIds() throws IOException {
        
        final int[] sum = new int[]{ 0, 0 };
        queryIndex.scan(new QueryIndex.QueryCollector() {
            @Override
            public void matchQuery(String id, QueryCacheEntry query, QueryIndex.DataValues dataValues) throws IOException {
                assertThat(query).isNotNull();
                assertThat(query.matchQuery).isInstanceOf(TermQuery.class);
                final String term = ((TermQuery)query.matchQuery).getTerm().text();
                assertThat(term).isEqualTo(id);
                sum[0] += Integer.valueOf(id);
                ++sum[1];
            }
        });
        return sum;
    }
    
    private List<Indexable> createUpdate(int id) {
        
        final String sid = String.valueOf(id);
        
        final BytesRef hash = new BytesRef(new byte[] {
                (byte)((id >> 24) & 0xff), (byte)((id >> 16) & 0xff),
                (byte)((id >> 8) & 0xff), (byte)(id & 0xff)
                });
        
        final Query query = new TermQuery(new Term("id", sid));

        final QueryCacheEntry queryCacheEntry = new QueryCacheEntry(hash, query, null); 
        
        final Document document = new Document();
        
        document.add(new StringField(FIELDS.del, sid, Field.Store.YES));
        document.add(new SortedDocValuesField(FIELDS.id, new BytesRef(sid)));
        document.add(new BinaryDocValuesField(FIELDS.hash, hash));

        final List<Indexable> updates = new ArrayList<>();
        
        updates.add(new Indexable(sid, queryCacheEntry, document));
        
        return updates;
    }
    
    private Term createDelete(int id) {
        return new Term(Monitor.FIELDS.del, String.valueOf(id));
    }
}
