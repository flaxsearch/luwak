package uk.co.flax.luwak.intervals;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.intervals.Interval;
import org.apache.lucene.search.intervals.IntervalCollector;
import org.apache.lucene.search.intervals.IntervalIterator;

/**
 * a specialized Collector that uses an {@link IntervalIterator} to collect
 * match positions from a Scorer.
 */
public class QueryIntervalsMatchCollector extends SimpleCollector implements IntervalCollector {

    private IntervalIterator positions;

    private final MatchBuilder matches;

    public QueryIntervalsMatchCollector(String queryId) {
        matches = new MatchBuilder(queryId);
    }

    public IntervalsQueryMatch getMatches() {
        return matches.build();
    }

    @Override
    public void collect(int doc) throws IOException {
        if (positions != null) {
            positions.scorerAdvanced(doc);
            while(positions.next() != null) {
                positions.collect(this);
            }
        }
        else {
            matches.setMatch();
        }
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
        try {
            positions = scorer.intervals(true);
        }
        catch (UnsupportedOperationException e) {
            // Query doesn't support positions, so we just say if it's a match or not
        }
    }

    @Override
    public void collectLeafPosition(Scorer scorer, Interval interval, int docID) {
        matches.addInterval(interval);
    }

    @Override
    public void collectComposite(Scorer scorer, Interval interval,
                                 int docID) {
        //offsets.add(new Offset(interval.begin, interval.end, interval.offsetBegin, interval.offsetEnd));
    }

    @Override
    public boolean needsScores() {
        return true;
    }

    @Override
    public boolean needsIntervals() {
        return true;
    }

    private static class MatchBuilder {

        private final Map<String, List<IntervalsQueryMatch.Hit>> hits = new HashMap<>();

        private boolean match;

        private String queryId;

        public MatchBuilder(String queryId) {
            this.queryId = queryId;
        }

        public void setMatch() {
            this.match = true;
        }

        public void addInterval(Interval interval) {
            if (!hits.containsKey(interval.field))
                hits.put(interval.field, new ArrayList<IntervalsQueryMatch.Hit>());
            hits.get(interval.field)
                    .add(new IntervalsQueryMatch.Hit(interval.begin, interval.offsetBegin, interval.end, interval.offsetEnd));
        }

        public IntervalsQueryMatch build() {
            if (!match && hits.size() == 0)
                return null;
            return new IntervalsQueryMatch(queryId, hits);
        }
    }

}
