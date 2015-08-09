package uk.co.flax.luwak.util;
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
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spans.SpanCollector;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanWeight;
import org.apache.lucene.search.spans.Spans;
import org.apache.lucene.util.PriorityQueue;

public class MultiSpans {

    List<SpanWeight> weights = new ArrayList<>();

    public MultiSpans(SpanQuery q, IndexSearcher searcher) throws IOException {
        q = (SpanQuery) q.rewrite(searcher.getIndexReader());
        weights.add(q.createWeight(searcher, false));
    }

    public Spans getSpans(LeafReaderContext context) throws IOException {
        return new CompositeSpans(context);
    }

    private class CompositeSpans extends Spans {

        SpanPriorityQueue queue = new SpanPriorityQueue(weights.size());
        List<Spans> subSpans = new ArrayList<>(weights.size());
        boolean cached = false;
        int doc = -1;

        CompositeSpans(LeafReaderContext ctx) throws IOException {
            for (SpanWeight w : weights) {
                Spans spans = w.getSpans(ctx, ctx.reader().getLiveDocs(), SpanWeight.Postings.OFFSETS);
                if (spans != null)
                    subSpans.add(spans);
            }
        }

        @Override
        public int advance(int doc) throws IOException {
            queue.clear();
            for (Spans spans : subSpans) {
                if (spans.advance(doc) == doc) {
                    spans.nextStartPosition();
                    queue.add(spans);
                }
            }
            if (queue.size() > 0)
                cached = true;
            return this.doc = doc;
        }

        @Override
        public int nextStartPosition() throws IOException {
            if (cached) {
                cached = false;
                return queue.top().startPosition();
            }

            if (queue.size() == 0 || queue.top().startPosition() == Spans.NO_MORE_POSITIONS)
                return Spans.NO_MORE_POSITIONS;

            queue.top().nextStartPosition();
            queue.updateTop();
            return queue.top().startPosition();
        }

        @Override
        public int startPosition() {
            return queue.top().startPosition();
        }

        @Override
        public int endPosition() {
            return queue.top().endPosition();
        }

        @Override
        public void collect(SpanCollector spanCollector) throws IOException {
            queue.top().collect(spanCollector);
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long cost() {
            throw new UnsupportedOperationException();
        }
    }

    private static class SpanPriorityQueue extends PriorityQueue<Spans> {

        public SpanPriorityQueue(int maxSize) {
            super(maxSize);
        }

        @Override
        protected boolean lessThan(Spans spans1, Spans spans2) {
            return spans1.startPosition() == spans2.startPosition() ?
                    spans1.endPosition() < spans2.endPosition() :
                    spans1.startPosition() < spans2.startPosition();
        }
    }
}
