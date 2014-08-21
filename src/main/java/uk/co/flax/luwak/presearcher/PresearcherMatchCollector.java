package uk.co.flax.luwak.presearcher;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.StoredDocument;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.intervals.Interval;
import org.apache.lucene.search.intervals.IntervalCollector;
import org.apache.lucene.search.intervals.IntervalIterator;
import org.apache.lucene.util.BytesRef;
import uk.co.flax.luwak.Monitor;
import uk.co.flax.luwak.TimedCollector;

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
public class PresearcherMatchCollector extends TimedCollector implements IntervalCollector {

    private IntervalIterator positions;
    private AtomicReader reader;
    private StoredDocument document;
    private BinaryDocValues idValues;
    private String currentId;

    public final Map<String, StringBuilder> matchingTerms = new HashMap<>();

    BytesRef scratch;

    @Override
    public void collect(int doc) throws IOException {

        document = reader.document(doc);
        scratch = idValues.get(doc);
        this.currentId = scratch.utf8ToString();

        positions.scorerAdvanced(doc);
        while(positions.next() != null) {
            positions.collect(this);
        }
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
    public void doSetNextReader(AtomicReaderContext context) throws IOException {
        this.reader = context.reader();
        this.idValues = this.reader.getBinaryDocValues(Monitor.FIELDS.id);
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return false;
    }

    @Override
    public void collectLeafPosition(Scorer scorer, Interval interval, int docID) {
        String terms = document.getField(interval.field).stringValue();
        if (!matchingTerms.containsKey(currentId))
            matchingTerms.put(currentId, new StringBuilder());
        matchingTerms.get(currentId)
                .append(" ")
                .append(interval.field)
                .append(":")
                .append(terms.substring(interval.offsetBegin, interval.offsetEnd));
    }

    @Override
    public void collectComposite(Scorer scorer, Interval interval, int docID) {

    }

    @Override
    public void setSearchTime(long l) {

    }
}
