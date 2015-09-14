package uk.co.flax.luwak.matchers;

import java.io.IOException;

import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.spans.SpanCollector;
import org.apache.lucene.search.spans.SpanExtractor;
import org.apache.lucene.search.spans.SpanRewriter;
import uk.co.flax.luwak.CandidateMatcher;
import uk.co.flax.luwak.InputDocument;
import uk.co.flax.luwak.MatcherFactory;

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
 * If a stored query does not support interval iterators, a {@link HighlightsMatch} object
 * with no Hit positions will be returned.
 *
 * If a query is matched, it will be run a second time against the highlight query (if
 * not null) to get positions.
 */

public class HighlightingMatcher extends CandidateMatcher<HighlightsMatch> {

    private final SpanRewriter rewriter;

    public HighlightingMatcher(InputDocument doc, SpanRewriter rewriter) {
        super(doc);
        this.rewriter = rewriter;
    }

    @Override
    protected HighlightsMatch doMatchQuery(String queryId, Query matchQuery) throws IOException {
        HighlightsMatch match = doMatch(queryId, matchQuery);
        if (match != null)
            this.addMatch(queryId, match);
        return match;
    }

    @Override
    protected void addMatch(String queryId, HighlightsMatch match) {
        HighlightsMatch previousMatch = this.matches(queryId);
        if (previousMatch == null) {
            super.addMatch(queryId, match);
            return;
        }
        super.addMatch(queryId, HighlightsMatch.merge(queryId, previousMatch, match));
    }

    public HighlightsMatch resolve(HighlightsMatch match1, HighlightsMatch match2) {
        return HighlightsMatch.merge(match1.getQueryId(), match1, match2);
    }

    private HighlightsMatch doMatch(String queryId, Query query) throws IOException {

        if (doc.getSearcher().count(query) == 0)
            return null;

        final HighlightsMatch match = new HighlightsMatch(queryId);
        final SpanCollector collector = new SpanCollector() {
            @Override
            public void collectLeaf(PostingsEnum postingsEnum, int pos, Term term) throws IOException {
                match.addHit(term.field(), pos, pos, postingsEnum.startOffset(), postingsEnum.endOffset());
            }

            @Override
            public void reset() {

            }
        };

        doc.getSearcher().search(rewriter.rewrite(query), new SimpleCollector() {

            Scorer scorer;

            @Override
            public void collect(int i) throws IOException {
                try {
                    SpanExtractor.collect(scorer, collector, true);
                }
                catch (Exception e) {
                    match.error = e.getMessage();
                }
            }

            @Override
            public void setScorer(Scorer scorer) throws IOException {
                this.scorer = scorer;
            }

            @Override
            public boolean needsScores() {
                return true;
            }
        });

        return match;
    }

    public static final MatcherFactory<HighlightsMatch> FACTORY = new MatcherFactory<HighlightsMatch>() {
        @Override
        public HighlightingMatcher createMatcher(InputDocument doc) {
            return new HighlightingMatcher(doc, new SpanRewriter());
        }
    };

    public static MatcherFactory<HighlightsMatch> factory(final SpanRewriter rewriter) {
        return new MatcherFactory<HighlightsMatch>() {
            @Override
            public CandidateMatcher<HighlightsMatch> createMatcher(InputDocument doc) {
                return new HighlightingMatcher(doc, rewriter);
            }
        };
    }

}
