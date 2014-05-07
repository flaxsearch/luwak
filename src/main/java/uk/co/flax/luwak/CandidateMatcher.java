package uk.co.flax.luwak;

import org.apache.lucene.search.Query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

public abstract class CandidateMatcher {

    private final List<MatchError> errors = new ArrayList<>();

    protected final InputDocument doc;

    private long queryBuildTime = -1;
    private long searchTime = -1;
    private int queriesRun = -1;

    public CandidateMatcher(InputDocument doc) {
        this.doc = doc;
    }

    public abstract void matchQuery(String queryId, Query matchQuery, Query highlightQuery) throws IOException;

    public abstract boolean matches(String queryId);

    public abstract int getMatchCount();

    public void reportError(MatchError e) {
        this.errors.add(e);
    }

    public List<MatchError> getErrors() {
        return errors;
    }

    public InputDocument getDocument() {
        return doc;
    }

    public String docId() {
        return doc.getId();
    }

    public long getQueryBuildTime() {
        return queryBuildTime;
    }

    void setQueryBuildTime(long queryBuildTime) {
        this.queryBuildTime = queryBuildTime;
    }

    public long getSearchTime() {
        return searchTime;
    }

    void setSearchTime(long searchTime) {
        this.searchTime = searchTime;
    }

    public int getQueriesRun() {
        return queriesRun;
    }

    void setQueriesRun(int queriesRun) {
        this.queriesRun = queriesRun;
    }

}
