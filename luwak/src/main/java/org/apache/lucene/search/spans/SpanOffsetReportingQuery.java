package org.apache.lucene.search.spans;

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
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;

/**
 * SpanQuery that wraps another SpanQuery, ensuring that offsets are loaded
 * from the postings lists and exposed to SpanCollectors.
 */
public class SpanOffsetReportingQuery extends SpanQuery {

    private final SpanQuery in;

    /**
     * Create a new SpanOffsetReportingQuery
     * @param in the query to wrap
     */
    public SpanOffsetReportingQuery(SpanQuery in) {
        this.in = in;
    }

    @Override
    public String getField() {
        return in.getField();
    }

    @Override
    public String toString(String field) {
        return in.toString();
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        SpanQuery rewritten = (SpanQuery) in.rewrite(reader);
        if (in.equals(rewritten))
            return this;
        return new SpanOffsetReportingQuery((SpanQuery)in.rewrite(reader));
    }

    @Override
    public SpanWeight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
        return new SpanOffsetWeight(searcher, in.createWeight(searcher, needsScores));
    }

    private class SpanOffsetWeight extends SpanWeight {

        private final SpanWeight in;

        private SpanOffsetWeight(IndexSearcher searcher, SpanWeight in) throws IOException {
            super(SpanOffsetReportingQuery.this, searcher, getTermContexts(in));
            this.in = in;
        }

        @Override
        public void extractTermContexts(Map<Term, TermContext> contexts) {
            in.extractTermContexts(contexts);
        }

        @Override
        public Spans getSpans(LeafReaderContext ctx, Postings requiredPostings) throws IOException {
            return in.getSpans(ctx, Postings.OFFSETS);
        }

        @Override
        public void extractTerms(Set<Term> terms) {
            in.extractTerms(terms);
        }
    }
}
