package uk.co.flax.luwak.matchers;

import java.io.IOException;

import org.apache.lucene.search.Scorer;
import uk.co.flax.luwak.DocumentBatch;
import uk.co.flax.luwak.MatcherFactory;
import uk.co.flax.luwak.QueryMatch;

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

public class SimpleMatcher extends CollectingMatcher<QueryMatch> {

    public SimpleMatcher(DocumentBatch docs) {
        super(docs);
    }

    @Override
    public QueryMatch resolve(QueryMatch match1, QueryMatch match2) {
        return match1;
    }

    @Override
    protected QueryMatch doMatch(String queryId, String docId, Scorer scorer) throws IOException {
        return new QueryMatch(queryId, docId);
    }


    @Override
    protected MatchCollector buildMatchCollector(String queryId) {
        return new SimpleMatchCollector(queryId);
    }


    public static final MatcherFactory<QueryMatch> FACTORY = SimpleMatcher::new;


    private class SimpleMatchCollector extends MatchCollector {
        public SimpleMatchCollector(String queryId) {
            super(queryId);
        }

        @Override
        public boolean needsScores() {
            return false;
        }
    }
}
