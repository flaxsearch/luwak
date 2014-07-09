package uk.co.flax.luwak;

import java.io.IOException;
import java.util.*;

import org.apache.lucene.search.Query;

/**
 * Copyright (c) 2014 Lemur Consulting Ltd.
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
 * Class used to match candidate queries selected by a Presearcher from a Monitor
 * query index.
 */
public abstract class CandidateMatcher<T extends QueryMatch> implements Iterable<T> {

    private final List<MatchError> errors = new ArrayList<>();
    private final Map<String, T> matches = new HashMap<>();

    protected final InputDocument doc;

    private long queryBuildTime = -1;
    private long searchTime = -1;
    private int queriesRun = -1;

    /**
     * Creates a new CandidateMatcher for the supplied InputDocument
     * @param doc the document to run queries against
     */
    public CandidateMatcher(InputDocument doc) {
        this.doc = doc;
    }

    /**
     * Runs the supplied query against this CandidateMatcher's InputDocument, storing any
     * resulting match.
     *
     * @param queryId the query id
     * @param matchQuery the query to run
     * @param highlightQuery an optional query to use for highlighting.  May be null
     * @throws IOException
     */
    public final void matchQuery(String queryId, Query matchQuery, Query highlightQuery) throws IOException {
        T match = doMatch(queryId, matchQuery, highlightQuery);
        if (match != null)
            matches.put(match.getQueryId(), match);
    }

    /**
     * Run the supplied query against this CandidateMatcher's InputDocument
     * @param queryId the query id
     * @param matchQuery the query to run
     * @param highlightQuery an optional query to use for highlighting.  May be null
     * @return a QueryMatch object if the query matched, otherwise null
     * @throws IOException
     */
    protected abstract T doMatch(String queryId, Query matchQuery, Query highlightQuery) throws IOException;

    /**
     * Returns true if a given query matched during the matcher run
     * @param queryId the query id
     * @return true if the query matched during the matcher run
     */
    public boolean matches(String queryId) {
        return matches.containsKey(queryId);
    }

    /**
     * @return the number of queries that matched
     */
    public int getMatchCount() {
        return matches.size();
    }

    @Override
    public Iterator<T> iterator() {
        return matches.values().iterator();
    }

    void reportError(MatchError e) {
        this.errors.add(e);
    }

    /**
     * @return a List of any MatchErrors created during the matcher run
     */
    public List<MatchError> getErrors() {
        return errors;
    }

    /**
     * @return the InputDocument for this CandidateMatcher
     */
    public InputDocument getDocument() {
        return doc;
    }

    /**
     * @return the id of the InputDocument for this CandidateMatcher
     */
    public String docId() {
        return doc.getId();
    }

    /**
     * @return how long (in ms) it took to build the Presearcher query for the matcher run
     */
    public long getQueryBuildTime() {
        return queryBuildTime;
    }

    void setQueryBuildTime(long queryBuildTime) {
        this.queryBuildTime = queryBuildTime;
    }

    /**
     * @return how long (in ms) it took to run the selected queries
     */
    public long getSearchTime() {
        return searchTime;
    }

    void setSearchTime(long searchTime) {
        this.searchTime = searchTime;
    }

    /**
     * @return the number of queries passed to this CandidateMatcher during the matcher run
     */
    public int getQueriesRun() {
        return queriesRun;
    }

    void setQueriesRun(int queriesRun) {
        this.queriesRun = queriesRun;
    }

}
