package uk.co.flax.luwak;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.intervals.Interval;
import org.apache.lucene.search.intervals.IntervalCollector;
import org.apache.lucene.search.intervals.IntervalIterator;

import java.io.IOException;

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

public class QueryMatchCollector extends Collector implements IntervalCollector {

    protected Scorer scorer;
    private IntervalIterator positions;

    private QueryMatch matches = null;
    private final String id;

    public QueryMatchCollector(String id) {
        this.id = id;
    }

    public QueryMatch getMatches() {
        return matches;
    }

    @Override
    public void collect(int doc) throws IOException {
        // consume any remaining positions the scorer didn't report
        matches = new QueryMatch(this.id);
        positions.scorerAdvanced(doc);
        while(positions.next() != null) {
            positions.collect(this);
        }
    }

    public boolean acceptsDocsOutOfOrder() {
        return false;
    }

    public void setScorer(Scorer scorer) throws IOException {
        this.scorer = scorer;
        positions = scorer.intervals(true);
        // If we want to visit the other scorers, we can, here...
    }

    public void setNextReader(AtomicReaderContext context) throws IOException {
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
