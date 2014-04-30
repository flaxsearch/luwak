package uk.co.flax.luwak.intervals;

import org.apache.lucene.search.Query;
import uk.co.flax.luwak.CandidateMatcher;
import uk.co.flax.luwak.InputDocument;

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
public class IntervalsMatcher extends CandidateMatcher {

    private final List<IntervalsQueryMatch> matches = new ArrayList<>();

    @Override
    public void matchQuery(InputDocument document, String id, Query matchQuery, Query highlightQuery) throws IOException {

        QueryIntervalsMatchCollector collector = new QueryIntervalsMatchCollector(id);
        document.getSearcher().search(matchQuery, collector);
        IntervalsQueryMatch hits = collector.getMatches();

        if (hits.getHitCount() == 0)
            return;

        if (highlightQuery == null) {
            matches.add(hits);
            return;
        }

        document.getSearcher().search(highlightQuery, collector);
        hits = collector.getMatches();
        if (hits.getHitCount() != 0)
            matches.add(hits);
    }

    public List<IntervalsQueryMatch> getMatches() {
        return matches;
    }
}
