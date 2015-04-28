package uk.co.flax.luwak;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.search.Query;

/*
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
public abstract class CandidateMatcher<T extends QueryMatch> {

    protected final InputDocument doc;
    protected long slowLogLimit;

    private final List<MatchError> errors = new ArrayList<>();
    private final Map<String, T> matches = new HashMap<>();

    private long queryBuildTime = -1;
    private long searchTime = System.nanoTime();
    private int queriesRun = -1;

    protected final StringBuilder slowlog = new StringBuilder();

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
     * @return a QueryMatch object if the query matched, otherwise null
     * @throws IOException on IO errors
     * @return true if the query matches
     */
    public abstract T matchQuery(String queryId, Query matchQuery, Query highlightQuery) throws IOException;

    protected void addMatch(String queryId, T match) {
        if (matches.containsKey(queryId))
            matches.put(queryId, resolve(match, matches.get(queryId)));
        else
            matches.put(queryId, match);
    }

    /**
     * If two matches from the same query are found (for example, two branches of a disjunction),
     * combine them.
     * @param match1 the first match found
     * @param match2 the second match found
     * @return a Match object that combines the two
     */
    public abstract T resolve(T match1, T match2);

    /**
     * Called by the Monitor if running a query throws an Exception
     * @param e the MatchError detailing the problem
     */
    public void reportError(MatchError e) {
        this.errors.add(e);
    }

    /**
     * @return the InputDocument for this CandidateMatcher
     */
    public InputDocument getDocument() {
        return doc;
    }

    public void finish(long buildTime, int queryCount) {
        this.queryBuildTime = buildTime;
        this.queriesRun = queryCount;
        this.searchTime = TimeUnit.MILLISECONDS.convert(System.nanoTime() - searchTime, TimeUnit.NANOSECONDS);
    }

    /*
     * Called by the Monitor
     */
    public void setSlowLogLimit(long t) {
        this.slowLogLimit = t;
    }

    /**
     * Returns the QueryMatch for the given query, or null if it did not match
     * @param queryId the query id
     * @return the QueryMatch for the given query, or null if it did not match
     */
    protected T matches(String queryId) {
        return matches.get(queryId);
    }

    public Matches<T> getMatches() {
        return new Matches<>(doc.getId(), matches, errors, queryBuildTime, searchTime, queriesRun, slowlog.toString());
    }
}
