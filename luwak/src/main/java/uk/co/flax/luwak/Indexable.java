package uk.co.flax.luwak;

import org.apache.lucene.document.Document;

/**
 * An indexable query to be added to the Monitor's queryindex
 */
public class Indexable {

    /** The id of the parent {@link MonitorQuery} */
    public final String id;

    /** The {@link QueryCacheEntry} to be indexed */
    public final QueryCacheEntry queryCacheEntry;

    /** A representation of the {@link QueryCacheEntry} as a lucene {@link Document} */
    public final Document document;

    public Indexable(String id, QueryCacheEntry queryCacheEntry, Document document) {
        this.id = id;
        this.queryCacheEntry = queryCacheEntry;
        this.document = document;
    }
}
