package uk.co.flax.luwak;

import org.apache.lucene.search.Query;

public class QueryCacheEntry {

    private final Query query;
    private final Query highlightQuery;

    public QueryCacheEntry(Query query, Query highlightQuery) {
        this.query = query;
        this.highlightQuery = highlightQuery;
    }

    public Query getQuery() {
        return query;
    }

    public Query getHighlightQuery() {
        return highlightQuery;
    }

    
}
