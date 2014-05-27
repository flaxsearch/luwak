package uk.co.flax.luwak.matchers;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import uk.co.flax.luwak.CandidateMatcher;
import uk.co.flax.luwak.InputDocument;
import uk.co.flax.luwak.MatcherFactory;
import uk.co.flax.luwak.QueryMatch;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

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
public class SimpleMatcher extends CandidateMatcher<QueryMatch> {

    private final Set<QueryMatch> matches = new HashSet<>();

    public SimpleMatcher(InputDocument doc) {
        super(doc);
    }

    @Override
    public void matchQuery(final String queryId, Query matchQuery, Query highlightQuery) throws IOException {
        doc.getSearcher().search(matchQuery, new Collector() {
            @Override
            public void setScorer(Scorer scorer) throws IOException {

            }

            @Override
            public void collect(int doc) throws IOException {
                matches.add(new QueryMatch(queryId));
            }

            @Override
            public void setNextReader(AtomicReaderContext context) throws IOException {

            }

            @Override
            public boolean acceptsDocsOutOfOrder() {
                return false;
            }
        });
    }

    @Override
    public boolean matches(String queryId) {
        return matches.contains(queryId);
    }

    @Override
    public int getMatchCount() {
        return matches.size();
    }

    public static final MatcherFactory<SimpleMatcher> FACTORY = new MatcherFactory<SimpleMatcher>() {
        @Override
        public SimpleMatcher createMatcher(InputDocument doc) {
            return new SimpleMatcher(doc);
        }
    };

    @Override
    public Iterator<QueryMatch> iterator() {
        return matches.iterator();
    }
}
