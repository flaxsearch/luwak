package uk.co.flax.luwak.intervals;

import java.io.IOException;

import org.apache.lucene.search.Query;
import uk.co.flax.luwak.CandidateMatcher;
import uk.co.flax.luwak.InputDocument;
import uk.co.flax.luwak.MatcherFactory;

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
public class IntervalsMatcher extends CandidateMatcher<IntervalsQueryMatch> {

    public IntervalsMatcher(InputDocument doc) {
        super(doc);
    }

    @Override
    public IntervalsQueryMatch doMatch(String queryId, Query matchQuery, Query highlightQuery) throws IOException {

        QueryIntervalsMatchCollector collector = new QueryIntervalsMatchCollector(queryId);
        doc.getSearcher().search(matchQuery, collector);
        IntervalsQueryMatch hits = collector.getMatches();

        if (hits.getHitCount() == 0)
            return null;

        if (highlightQuery == null) {
            return hits;
        }

        QueryIntervalsMatchCollector collector2 = new QueryIntervalsMatchCollector(queryId);
        doc.getSearcher().search(highlightQuery, collector2);
        IntervalsQueryMatch hlhits = collector2.getMatches();

        if (hlhits.getHitCount() != 0)
            return hlhits;
        else
            return hits;
    }

    public static final MatcherFactory<IntervalsMatcher> FACTORY = new MatcherFactory<IntervalsMatcher>() {
        @Override
        public IntervalsMatcher createMatcher(InputDocument doc) {
            return new IntervalsMatcher(doc);
        }
    };

}
