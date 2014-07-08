package uk.co.flax.luwak;

import java.io.IOException;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.BytesRef;

/**
 * A Collector that decodes the stored query for each document hit.
 */
public abstract class MonitorQueryCollector extends TimedCollector {

    protected BinaryDocValues queryDV;
    protected BinaryDocValues highlightDV;
    protected BinaryDocValues idDV;
    protected NumericDocValues seqIdDV;

    final BytesRef query = new BytesRef();
    final BytesRef highlight = new BytesRef();
    final BytesRef id = new BytesRef();
    long seqId = 0;

    /**
     * Do something with the matching query
     *
     * @param query MonitorQuery instance
     */
    protected abstract void doSearch(MonitorQuery query);

    /**
     * Finish collecting
     */
    protected abstract void finish();

    private int queryCount = 0;
    private long searchTime = -1;

    @Override
    public void setScorer(Scorer scorer) throws IOException {

    }

    @Override
    public final void collect(int doc) throws IOException {
        queryDV.get(doc, query);
        highlightDV.get(doc, highlight);
        idDV.get(doc, id);
        seqId = seqIdDV.get(doc);
        queryCount++;
        MonitorQuery q = new MonitorQuery(id.utf8ToString(), query, highlight, seqId);
        doSearch(q);
    }

    @Override
    public final void setNextReader(AtomicReaderContext context) throws IOException {
        this.queryDV = context.reader().getBinaryDocValues(Monitor.FIELDS.query);
        this.highlightDV = context.reader().getBinaryDocValues(Monitor.FIELDS.highlight);
        this.idDV = context.reader().getBinaryDocValues(Monitor.FIELDS.id);
        this.seqIdDV = context.reader().getNumericDocValues(Monitor.FIELDS.seqId);
    }

    @Override
    public final boolean acceptsDocsOutOfOrder() {
        return true;
    }

    public int getQueryCount() {
        return queryCount;
    }

    public long getSearchTime() {
        return searchTime;
    }

    @Override
    public void setSearchTime(long searchTime) {
        this.searchTime = searchTime;
    }
}
