package uk.co.flax.luwak;

import java.util.List;

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
 * Summary of all matches for an individual {@link uk.co.flax.luwak.InputDocument} run
 * against a Monitor.
 */
public class DocumentMatches {

    private final String id;
    private final List<QueryMatch> matches;
    private final List<MatchError> errors;
    private final MatchStats stats;

    public DocumentMatches(String docId, List<QueryMatch> matches, List<MatchError> errors,
                           int qcount, long preptime, long querytime) {
        this.id = docId;
        this.matches = matches;
        this.errors = errors;
        this.stats = new MatchStats(qcount, preptime, querytime);
    }

    /**
     * @return the id of the {@link InputDocument} this object is for
     */
    public String docId() {
        return id;
    }

    /**
     * @return a list of {@link QueryMatch} objects, one for each query that matched the document
     */
    public List<QueryMatch> matches() {
        return matches;
    }

    /**
     * @return a list of errors encountered when monitoring the document
     */
    public List<MatchError> errors() {
        return errors;
    }

    /**
     * @return the {@link MatchStats} for this run
     */
    public MatchStats getMatchStats() {
        return stats;
    }

    /**
     * Statistics for a {@link Monitor#match(InputDocument)} call
     */
    public static class MatchStats {

        /** Time taken to prepare the document for searching */
        public final long preptime;

        /** Total time taken running queries against the document */
        public final long querytime;

        /** Total number of queries run against the document */
        public final int querycount;

        /** Constructs a new MatchStats instance */
        public MatchStats(int querycount, long preptime, long querytime) {
            this.querycount = querycount;
            this.preptime = preptime;
            this.querytime = querytime;
        }
    }
}
