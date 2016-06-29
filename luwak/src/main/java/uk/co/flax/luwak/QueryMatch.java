package uk.co.flax.luwak;

/*
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

import java.util.Objects;

/**
 * Represents a match for a specific query and document
 *
 * Derived classes may contain more information (such as scores, highlights, etc)
 *
 * @see uk.co.flax.luwak.matchers.ExplainingMatch
 * @see uk.co.flax.luwak.matchers.ScoringMatch
 * @see uk.co.flax.luwak.matchers.HighlightsMatch
 */
public class QueryMatch {

    private final String queryId;

    private final String docId;

    /**
     * Creates a new QueryMatch for a specific query and document
     * @param queryId the query id
     * @param docId the document id
     */
    public QueryMatch(String queryId, String docId) {
        this.queryId = Objects.requireNonNull(queryId);
        this.docId = Objects.requireNonNull(docId);
    }

    /**
     * @return the queryid of this match
     */
    public String getQueryId() {
        return queryId;
    }

    /**
     * @return the docid of this match
     */
    public String getDocId() {
        return docId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof QueryMatch)) return false;
        QueryMatch that = (QueryMatch) o;
        return Objects.equals(queryId, that.queryId) && Objects.equals(docId, that.docId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queryId, docId);
    }

    @Override
    public String toString() {
        return "Match(doc=" + docId + ",query=" + queryId + ")";
    }
}
