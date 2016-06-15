package uk.co.flax.luwak.benchmark;
/*
 *   Copyright (c) 2015 Lemur Consulting Ltd.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.search.Query;
import uk.co.flax.luwak.CandidateMatcher;
import uk.co.flax.luwak.DocumentBatch;
import uk.co.flax.luwak.InputDocument;
import uk.co.flax.luwak.MatcherFactory;

/**
 * Matcher class that returns a match from all queries selected by the Presearcher
 *
 * Useful for checking presearcher timings
 */
public class PresearcherMatcher extends CandidateMatcher<PresearcherMatch> {

    public static final MatcherFactory<PresearcherMatch> FACTORY = new MatcherFactory<PresearcherMatch>() {
        @Override
        public CandidateMatcher<PresearcherMatch> createMatcher(DocumentBatch docs) {
            return new PresearcherMatcher(docs);
        }
    };

    public PresearcherMatcher(DocumentBatch batch) {
        super(batch);
    }

    @Override
    protected void doMatchQuery(String queryId, Query matchQuery, Map<String, String> metadata) throws IOException {
        for (InputDocument doc : docs) {
            this.addMatch(new PresearcherMatch(queryId, doc.getId()));
        }
    }

    @Override
    public PresearcherMatch resolve(PresearcherMatch match1, PresearcherMatch match2) {
        return match1;
    }

}
