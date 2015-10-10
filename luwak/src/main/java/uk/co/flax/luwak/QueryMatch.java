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

/**
 * Represents a match for a specific query
 *
 * Derived classes may contain more information (such as scores, highlights, etc)
 *
 * @see uk.co.flax.luwak.matchers.ExplainingMatch
 * @see uk.co.flax.luwak.matchers.ScoringMatch
 * @see uk.co.flax.luwak.matchers.HighlightsMatch
 */
public class QueryMatch {

    private final String queryId;

    /**
     * Creates a new QueryMatch for a specific query
     * @param queryId the query id
     */
    public QueryMatch(String queryId) {
        this.queryId = queryId;
    }

    /**
     * @return the queryid of this match
     */
    public String getQueryId() {
        return queryId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof QueryMatch)) return false;

        QueryMatch that = (QueryMatch) o;

        if (queryId != null ? !queryId.equals(that.queryId) : that.queryId != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return queryId != null ? queryId.hashCode() : 0;
    }
}
