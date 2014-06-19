package uk.co.flax.luwak.intervals;

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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.intervals.Interval;
import org.apache.lucene.search.intervals.IntervalCollector;
import org.apache.lucene.search.intervals.IntervalIterator;

import java.io.IOException;
import uk.co.flax.luwak.QueryMatch;

/**
 * a specialized Collector that uses an {@link IntervalIterator} to collect
 * match positions from a Scorer.
 */
public class QueryIntervalsMatchCollector extends Collector implements IntervalCollector {

    private IntervalIterator positions;

    private final IntervalsQueryMatch matches;

    public QueryIntervalsMatchCollector(String queryId) {
        matches = new IntervalsQueryMatch(queryId);
    }

    public IntervalsQueryMatch getMatches() {
        return matches;
    }

    @Override
    public void collect(int doc) throws IOException {
        if (positions != null) {
            positions.scorerAdvanced(doc);
            while(positions.next() != null) {
                positions.collect(this);
            }
        }
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {

    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return false;
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
        positions = scorer.intervals(true);
    }

    @Override
    public Weight.PostingFeatures postingFeatures() {
        return Weight.PostingFeatures.OFFSETS;
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

}
