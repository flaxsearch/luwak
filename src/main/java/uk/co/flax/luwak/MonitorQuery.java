package uk.co.flax.luwak;

import org.apache.lucene.util.BytesRef;

/**
 * Copyright (c) 2013 Lemur Consulting Ltd.
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
 * Defines a query to be stored in a Monitor
 */
public class MonitorQuery {

    private final String id;
    private final BytesRef query;
    private final BytesRef highlightQuery;

    /**
     * Creates a new MonitorQuery
     * @param id the ID
     * @param query the query to store
     * @param highlightQuery an optional query to use for highlighting.  May be null.
     */
    public MonitorQuery(String id, BytesRef query, BytesRef highlightQuery) {
        this.id = id;
        this.query = query;
        this.highlightQuery = highlightQuery;
    }

    /**
     * Creates a new MonitorQuery
     *
     * @param id the ID
     * @param query the query to store
     * @param highlightQuery an optional query to use for highlighting. May be null.
     */
    public MonitorQuery(String id, String query, String highlightQuery) {
        this.id = id;
        if (query == null) {
            throw new IllegalArgumentException("Null queries are not allowed");
        }
        this.query = new BytesRef(query);
        if (highlightQuery != null) {
            this.highlightQuery = new BytesRef(highlightQuery);
        } else {
            this.highlightQuery = null;
        }
    }

    /**
     * Creates a new MonitorQuery, with no highlight query
     * @param id the ID
     * @param query the query to store
     */
    public MonitorQuery(String id, String query) {
        this(id, query, null);
    }

    /**
     * @return this MonitorQuery's ID
     */
    public String getId() {
        return id;
    }

    /**
     * @return this MonitorQuery's query
     */
    public BytesRef getQuery() {
        return query;
    }

    /**
     * @return this MonitorQuery's highlight query.  May be null.
     */
    public BytesRef getHighlightQuery() {
        return highlightQuery;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MonitorQuery that = (MonitorQuery) o;

        if (highlightQuery != null ? !highlightQuery.equals(that.highlightQuery) : that.highlightQuery != null)
            return false;
        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        if (query != null ? !query.equals(that.query) : that.query != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (query != null ? query.hashCode() : 0);
        result = 31 * result + (highlightQuery != null ? highlightQuery.hashCode() : 0);
        return result;
    }
}
