package uk.co.flax.luwak.intervals;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanCollector;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.Spans;
import uk.co.flax.luwak.CandidateMatcher;
import uk.co.flax.luwak.InputDocument;
import uk.co.flax.luwak.MatcherFactory;
import uk.co.flax.luwak.util.MultiSpans;

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

/**
 * CandidateMatcher class that will return exact hit positions for all matching queries
 *
 * If a stored query does not support interval iterators, an IntervalsQueryMatch object
 * with no Hit positions will be returned.
 *
 * If a query is matched, it will be run a second time against the highlight query (if
 * not null) to get positions.
 */

public class IntervalsMatcher extends CandidateMatcher<IntervalsQueryMatch> {

    public IntervalsMatcher(InputDocument doc) {
        super(doc);
    }

    @Override
    public IntervalsQueryMatch matchQuery(String queryId, Query matchQuery, List<SpanQuery> highlightQuery) throws IOException {
        IntervalsQueryMatch match = doMatch(queryId, matchQuery, highlightQuery);
        if (match != null)
            this.addMatch(queryId, match);
        return match;
    }

    @Override
    protected void addMatch(String queryId, IntervalsQueryMatch match) {
        IntervalsQueryMatch previousMatch = this.matches(queryId);
        if (previousMatch == null) {
            super.addMatch(queryId, match);
            return;
        }
        super.addMatch(queryId, IntervalsQueryMatch.merge(queryId, previousMatch, match));
    }

    public IntervalsQueryMatch resolve(IntervalsQueryMatch match1, IntervalsQueryMatch match2) {
        return IntervalsQueryMatch.merge(match1.getQueryId(), match1, match2);
    }

    private IntervalsQueryMatch doMatch(String queryId, Query matchQuery, List<SpanQuery> highlightQuery) throws IOException {

        if (doc.getSearcher().count(matchQuery) == 0)
            return null;

        MultiSpans multiSpans = new MultiSpans(highlightQuery, doc.getSearcher());
        Spans spans = multiSpans.getSpans(doc.asAtomicReader().getContext());

        IntervalsQueryMatch match = new IntervalsQueryMatch(queryId);
        SpanOffsetsCollector collector = new SpanOffsetsCollector();

        spans.advance(0);
        while (spans.nextStartPosition() != Spans.NO_MORE_POSITIONS) {
            collector.reset();
            spans.collect(collector);
            match.addHit(collector.field, collector.startpos, collector.endpos, collector.startoffset, collector.endoffset);
        }

        return match;
    }

    private static class SpanOffsetsCollector implements SpanCollector {

        int startpos = Integer.MAX_VALUE;
        int startoffset = Integer.MAX_VALUE;
        int endpos = -1;
        int endoffset = -1;
        String field;

        @Override
        public void collectLeaf(PostingsEnum postingsEnum, int pos, Term term) throws IOException {
            startpos = Math.min(startpos, pos);
            endpos = Math.max(endpos, pos);
            startoffset = Math.min(postingsEnum.startOffset(), startoffset);
            endoffset = Math.max(postingsEnum.endOffset(), endoffset);
            field = term.field();
        }

        @Override
        public void reset() {
            startpos = startoffset = Integer.MAX_VALUE;
            endpos = endoffset = -1;
        }
    }

    public static final MatcherFactory<IntervalsQueryMatch> FACTORY = new MatcherFactory<IntervalsQueryMatch>() {
        @Override
        public IntervalsMatcher createMatcher(InputDocument doc) {
            return new IntervalsMatcher(doc);
        }
    };

}
