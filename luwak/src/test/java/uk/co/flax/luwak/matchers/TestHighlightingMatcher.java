package uk.co.flax.luwak.matchers;

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.complexPhrase.ComplexPhraseQueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import uk.co.flax.luwak.*;
import uk.co.flax.luwak.presearcher.MatchAllPresearcher;
import uk.co.flax.luwak.queryparsers.LuceneQueryParser;
import static uk.co.flax.luwak.assertions.HighlightingMatchAssert.assertThat;

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
public class TestHighlightingMatcher {

    static final String textfield = "textfield";

    static final Analyzer WHITESPACE = new WhitespaceAnalyzer();

    private Monitor monitor;

    public static InputDocument buildDoc(String id, String text) {
        return InputDocument.builder(id)
                .addField(textfield, text, WHITESPACE)
                .build();
    }

    @Before
    public void setUp() throws IOException {
        monitor = new Monitor(new LuceneQueryParser(textfield), new MatchAllPresearcher());
    }

    @After
    public void testDown() throws IOException {
        monitor.close();
    }

    @Test
    public void singleTermQueryMatchesSingleDocument() throws IOException, UpdateException {

        MonitorQuery mq = new MonitorQuery("query1", "test");
        monitor.update(mq);

        Matches<HighlightsMatch> matcher = monitor.match(buildDoc("doc1", "this is a test document"),
                HighlightingMatcher.FACTORY);

        assertThat(matcher)
                .hasMatchCount("doc1", 1)
                .matchesQuery("query1", "doc1")
                    .inField(textfield)
                        .withHit(new HighlightsMatch.Hit(3, 10, 3, 14));
    }

    @Test
    public void singlePhraseQueryMatchesSingleDocument() throws IOException, UpdateException {

        MonitorQuery mq = new MonitorQuery("query1", "\"test document\"");
        monitor.update(mq);

        Matches<HighlightsMatch> matcher = monitor.match(buildDoc("doc1", "this is a test document"),
                HighlightingMatcher.FACTORY);

        assertThat(matcher)
                .hasMatchCount("doc1", 1)
                .matchesQuery("query1", "doc1")
                .inField(textfield)
                .withHit(new HighlightsMatch.Hit(3, 10, 3, 14))
                .withHit(new HighlightsMatch.Hit(4, 15, 4, 23));
    }

    @Test
    public void testToString() {

        HighlightsMatch match = new HighlightsMatch("1", "1");
        match.addHit("field", 2, 3, -1, -1);
        match.addHit("field", 0, 1, -1, -1);
        match.addHit("afield", 0, 1, 0, 4);

        Assertions.assertThat(match.toString())
                .isEqualTo("Match(doc=1,query=1){hits={afield=[0(0)->1(4)], field=[0(-1)->1(-1), 2(-1)->3(-1)]}}");
    }

    @Test
    public void multiFieldQueryMatches() throws IOException, UpdateException {

        InputDocument doc = InputDocument.builder("doc1")
                .addField("field1", "this is a test of field one", WHITESPACE)
                .addField("field2", "and this is an additional test", WHITESPACE)
                .build();

        monitor.update(new MonitorQuery("query1", "field1:test field2:test"));

        Matches<HighlightsMatch> matcher = monitor.match(doc, HighlightingMatcher.FACTORY);

        assertThat(matcher)
                .hasMatchCount("doc1", 1)
                .matchesQuery("query1", "doc1")
                    .inField("field1")
                        .withHit(new HighlightsMatch.Hit(3, 10, 3, 14))
                    .inField("field2")
                        .withHit(new HighlightsMatch.Hit(5, 26, 5, 30));
    }

    @Test
    public void testQueryErrors() throws IOException, UpdateException {

        monitor = new Monitor((queryString, metadata) -> {
            if (queryString.equals("error!")) {
                return new Query() {
                    @Override
                    public String toString(String field) {
                        return "";
                    }

                    @Override
                    public Query rewrite(IndexReader reader) throws IOException {
                        throw new RuntimeException("Oops!");
                    }

                    @Override
                    public boolean equals(Object o) {
                        return false;
                    }

                    @Override
                    public int hashCode() {
                        return 0;
                    }
                };
            }
            return new LuceneQueryParser(textfield).parse(queryString, metadata);
        }, new MatchAllPresearcher());

        monitor.update(new MonitorQuery("1", "test"),
                new MonitorQuery("2", "error!"),
                new MonitorQuery("3", "document"),
                new MonitorQuery("4", "foo"));

        assertThat(monitor.match(buildDoc("doc1", "this is a test document"), HighlightingMatcher.FACTORY))
                .hasQueriesRunCount(4)
                .hasMatchCount("doc1", 2)
                .hasErrorCount(1);
    }

    @Test
    public void testWildcards() throws IOException, UpdateException {

        monitor = new Monitor((queryString, metadata) -> new RegexpQuery(new Term(textfield, "he.*")), new MatchAllPresearcher());

        monitor.update(new MonitorQuery("1", ""));

        Matches<HighlightsMatch> matches = monitor.match(buildDoc("1", "hello world"), HighlightingMatcher.FACTORY);
        assertThat(matches)
                .hasQueriesRunCount(1)
                .hasMatchCount("1", 1);

        Assertions.assertThat(matches.matches("1", "1").getHitCount()).isEqualTo(1);
    }

    @Test
    public void testWildcardCombinations() throws Exception {

        final BooleanQuery bq = new BooleanQuery.Builder()
                .add(new TermQuery(new Term(textfield, "term1")), BooleanClause.Occur.MUST)
                .add(new PrefixQuery(new Term(textfield, "term2")), BooleanClause.Occur.MUST)
                .add(new TermQuery(new Term(textfield, "term3")), BooleanClause.Occur.MUST_NOT)
                .build();

        monitor = new Monitor((queryString, metadata) -> bq, new MatchAllPresearcher());
        monitor.update(new MonitorQuery("1", ""));

        Matches<HighlightsMatch> matches = monitor.match(buildDoc("1", "term1 term22 term4"), HighlightingMatcher.FACTORY);
        assertThat(matches)
                .matchesQuery("1", "1")
                .withHitCount(2);
    }

    @Test
    public void testDisjunctionMaxQuery() throws IOException, UpdateException {
        final DisjunctionMaxQuery query = new DisjunctionMaxQuery(Arrays.asList(
                new TermQuery(new Term(textfield, "term1")), new PrefixQuery(new Term(textfield, "term2"))
        ), 1.0f);

        monitor = new Monitor((queryString, metadata) -> query, new MatchAllPresearcher());

        monitor.update(new MonitorQuery("1", ""));
        Matches<HighlightsMatch> matches = monitor.match(buildDoc("1", "term1 term2 term3"), HighlightingMatcher.FACTORY);

        assertThat(matches)
                .matchesQuery("1", "1")
                .withHitCount(2);
    }

    @Test
    public void testIdenticalMatches() throws Exception {

        final BooleanQuery bq = new BooleanQuery.Builder()
                .add(new TermQuery(new Term(textfield, "term1")), BooleanClause.Occur.MUST)
                .add(new TermQuery(new Term(textfield, "term1")), BooleanClause.Occur.SHOULD)
                .build();

        monitor = new Monitor((queryString, metadata) -> bq, new MatchAllPresearcher());

        monitor.update(new MonitorQuery("1", ""));
        Matches<HighlightsMatch> matches = monitor.match(buildDoc("1", "term1 term2"), HighlightingMatcher.FACTORY);

        assertThat(matches).matchesQuery("1", "1").withHitCount(1);
    }

    @Test
    public void testWildcardBooleanRewrites() throws Exception {

        final Query wc = new PrefixQuery(new Term(textfield, "term1"));

        final Query wrapper = new BooleanQuery.Builder()
                .add(wc, BooleanClause.Occur.MUST)
                .build();

        final Query wrapper2  = new BooleanQuery.Builder()
                .add(wrapper, BooleanClause.Occur.MUST)
                .build();

        final BooleanQuery bq = new BooleanQuery.Builder()
                .add(new PrefixQuery(new Term(textfield, "term2")), BooleanClause.Occur.MUST)
                .add(wrapper2, BooleanClause.Occur.MUST_NOT)
                .build();

        monitor = new Monitor((queryString, metadata) -> bq, new MatchAllPresearcher());

        monitor.update(new MonitorQuery("1", ""));
        Matches<HighlightsMatch> matches = monitor.match(buildDoc("1", "term2 term"), HighlightingMatcher.FACTORY);
        assertThat(matches).matchesQuery("1", "1").withHitCount(1);

        matches = monitor.match(buildDoc("1", "term2 term"), HighlightingMatcher.FACTORY);
        assertThat(matches).matchesQuery("1", "1").withHitCount(1);
    }

    @Test
    public void testWildcardProximityRewrites() throws Exception {
        final SpanNearQuery snq = SpanNearQuery.newOrderedNearQuery(textfield)
                .addClause(new SpanMultiTermQueryWrapper<>(new WildcardQuery(new Term(textfield, "term*"))))
                .addClause(new SpanTermQuery(new Term(textfield, "foo")))
                .build();

        monitor = new Monitor((queryString, metadata) -> snq, new MatchAllPresearcher());

        monitor.update(new MonitorQuery("1", ""));

        Matches<HighlightsMatch> matches = monitor.match(buildDoc("1", "term1 foo"), HighlightingMatcher.FACTORY);
        assertThat(matches).matchesQuery("1", "1").withHitCount(2);
    }

    @Test
    public void testDisjunctionWithOrderedNearSpans() throws Exception {

        final Query bq = new BooleanQuery.Builder()
                .add(new TermQuery(new Term(textfield, "a")), BooleanClause.Occur.SHOULD)
                .add(SpanNearQuery.newOrderedNearQuery(textfield)
                        .addClause(new SpanTermQuery(new Term(textfield, "b")))
                        .addClause(new SpanTermQuery(new Term(textfield, "c")))
                        .setSlop(1)
                        .build(), BooleanClause.Occur.SHOULD)
                .build();
        final Query parent = new BooleanQuery.Builder()
                .add(new TermQuery(new Term(textfield, "a")), BooleanClause.Occur.MUST)
                .add(bq, BooleanClause.Occur.MUST)
                .build();

        monitor = new Monitor((queryString, metadata) -> parent, new MatchAllPresearcher());
        monitor.update(new MonitorQuery("1", ""));

        InputDocument doc = buildDoc("1", "a b x x x x c");
        Matches<HighlightsMatch> matches = monitor.match(doc, HighlightingMatcher.FACTORY);

        assertThat(matches).matchesQuery("1", "1").withHitCount(1);

    }

    @Test
    public void testDisjunctionWithUnorderedNearSpans() throws Exception {

        final Query bq = new BooleanQuery.Builder()
                .add(new TermQuery(new Term(textfield, "a")), BooleanClause.Occur.SHOULD)
                .add(SpanNearQuery.newUnorderedNearQuery(textfield)
                        .addClause(new SpanTermQuery(new Term(textfield, "b")))
                        .addClause(new SpanTermQuery(new Term(textfield, "c")))
                        .setSlop(1)
                        .build(), BooleanClause.Occur.SHOULD)
                .build();
        final Query parent = new BooleanQuery.Builder()
                .add(new TermQuery(new Term(textfield, "a")), BooleanClause.Occur.MUST)
                .add(bq, BooleanClause.Occur.MUST)
                .build();

        monitor = new Monitor((queryString, metadata) -> parent, new MatchAllPresearcher());
        monitor.update(new MonitorQuery("1", ""));

        InputDocument doc = buildDoc("1", "a b x x x x c");
        Matches<HighlightsMatch> matches = monitor.match(doc, HighlightingMatcher.FACTORY);

        assertThat(matches).matchesQuery("1", "1").withHitCount(1);

    }

    @Test
    public void testEquality() {

        HighlightsMatch m1 = new HighlightsMatch("1", "1");
        m1.addHit("field", 0, 1, 0, 1);

        HighlightsMatch m2 = new HighlightsMatch("1", "1");
        m2.addHit("field", 0, 1, 0, 1);

        HighlightsMatch m3 = new HighlightsMatch("1", "1");
        m3.addHit("field", 0, 2, 0, 1);

        HighlightsMatch m4 = new HighlightsMatch("2", "1");
        m4.addHit("field", 0, 1, 0, 1);

        Assertions.assertThat(m1).isEqualTo(m2);
        Assertions.assertThat(m1.hashCode()).isEqualTo(m2.hashCode());

        Assertions.assertThat(m1).isNotEqualTo(m3);
        Assertions.assertThat(m1).isNotEqualTo(m4);
    }

    @Test
    public void testUnrewritableQuery() throws IOException, UpdateException {

        TermQuery inner = new TermQuery(new Term(textfield, "a"));
        monitor = new Monitor((q, m) -> new Query(){
            @Override
            public String toString(String s) {
                return "test";
            }

            @Override
            public boolean equals(Object o) {
                return false;
            }

            @Override
            public int hashCode() {
                return 0;
            }

            @Override
            public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
                return inner.createWeight(searcher, needsScores);
            }
        }, new MatchAllPresearcher());

        monitor.update(new MonitorQuery("1", ""));

        InputDocument doc = buildDoc("doc", "a b c");
        Matches<HighlightsMatch> matches = monitor.match(doc, HighlightingMatcher.FACTORY);

        assertThat(matches)
                .matchesQuery("1", "doc")
                .withErrorMessage("Don't know how to rewrite");

    }

    @Test
    public void testMutliValuedFieldWithNonDefaultGaps() throws IOException, UpdateException {

        Analyzer analyzer = new Analyzer() {
            @Override
            public int getPositionIncrementGap(String fieldName) {
                return 1000;
            }

            @Override
            public int getOffsetGap(String fieldName) {
                return 2000;
            }

            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                return new TokenStreamComponents(new WhitespaceTokenizer());
            }
        };

        MonitorQuery mq = new MonitorQuery("query", textfield + ":\"hello world\"~5");
        monitor.update(mq);

        InputDocument doc1 = InputDocument.builder("doc1")
                .addField(textfield, "hello world", analyzer)
                .addField(textfield, "goodbye", analyzer)
                .build();
        Matches<HighlightsMatch> matcher1 = monitor.match(doc1, HighlightingMatcher.FACTORY);
        assertThat(matcher1)
                .hasMatchCount("doc1", 1)
                .matchesQuery("query", "doc1")
                    .inField(textfield)
                        .withHit(new HighlightsMatch.Hit(0, 0, 0, 5))
                        .withHit(new HighlightsMatch.Hit(1, 6, 1, 11));

        InputDocument doc2 = InputDocument.builder("doc2")
                .addField(textfield, "hello", analyzer)
                .addField(textfield, "world", analyzer)
                .build();
        Matches<HighlightsMatch> matcher2 = monitor.match(doc2, HighlightingMatcher.FACTORY);
        assertThat(matcher2)
                .doesNotMatchQuery("query", "doc2")
                .hasMatchCount("doc2", 0);

        InputDocument doc3 = InputDocument.builder("doc3")
                .addField(textfield, "hello world", analyzer)
                .addField(textfield, "hello goodbye world", analyzer)
                .build();
        Matches<HighlightsMatch> matcher3 = monitor.match(doc3, HighlightingMatcher.FACTORY);
        assertThat(matcher3)
                .hasMatchCount("doc3", 1)
                .matchesQuery("query", "doc3")
                    .inField(textfield)
                        .withHit(new HighlightsMatch.Hit(0, 0, 0, 5))
                        .withHit(new HighlightsMatch.Hit(1, 6, 1, 11))
                        .withHit(new HighlightsMatch.Hit(1002, 2011, 1002, 2016))
                        .withHit(new HighlightsMatch.Hit(1004, 2025, 1004, 2030));

    }

    @Test
    public void testDisjunctionWithOrderedNearMatch() throws Exception {

        final Query bq = new BooleanQuery.Builder()
                .add(new TermQuery(new Term(textfield, "a")), BooleanClause.Occur.SHOULD)
                .add(SpanNearQuery.newOrderedNearQuery(textfield)
                        .addClause(new SpanTermQuery(new Term(textfield, "b")))
                        .addClause(new SpanTermQuery(new Term(textfield, "c")))
                        .setSlop(1)
                        .build(), BooleanClause.Occur.SHOULD)
                .build();
        final Query parent = new BooleanQuery.Builder()
                .add(new TermQuery(new Term(textfield, "a")), BooleanClause.Occur.MUST)
                .add(bq, BooleanClause.Occur.MUST)
                .build();

        monitor = new Monitor((queryString, metadata) -> parent, new MatchAllPresearcher());
        monitor.update(new MonitorQuery("1", ""));

        InputDocument doc = buildDoc("1", "a b c");
        Matches<HighlightsMatch> matches = monitor.match(doc, HighlightingMatcher.FACTORY);

        assertThat(matches)
        .matchesQuery("1", "1")
            .withHitCount(3)
            .inField(textfield)
                .withHit(new HighlightsMatch.Hit(0, 0, 0, 1))
                .withHit(new HighlightsMatch.Hit(1, 2, 1, 3))
                .withHit(new HighlightsMatch.Hit(2, 4, 2, 5));

    }

    @Test
    public void testUnorderedNearWithinOrderedNear() throws Exception {

        final SpanQuery spanPhrase = SpanNearQuery.newOrderedNearQuery(textfield)
                .addClause(new SpanTermQuery(new Term(textfield, "time")))
                .addClause(new SpanTermQuery(new Term(textfield,"men")))
                .setSlop(1)
                .build();

        final SpanQuery unorderedNear = SpanNearQuery.newUnorderedNearQuery(textfield)
                .addClause(spanPhrase)
                .addClause(new SpanTermQuery(new Term(textfield, "all")))
                .setSlop(5)
                .build();

        final SpanQuery orderedNear = SpanNearQuery.newOrderedNearQuery(textfield)
                .addClause(new SpanTermQuery(new Term(textfield, "the")))
                .addClause(unorderedNear)
                .setSlop(10)
                .build();

        final Query innerConjunct = new BooleanQuery.Builder()
                .add(new TermQuery(new Term(textfield, "is")), BooleanClause.Occur.MUST)
                .add(orderedNear, BooleanClause.Occur.MUST)
                .build();

        final Query disjunct = new BooleanQuery.Builder()
                .add(new TermQuery(new Term(textfield, "now")), BooleanClause.Occur.SHOULD)
                .add(innerConjunct, BooleanClause.Occur.SHOULD)
                .build();

        final Query outerConjunct = new BooleanQuery.Builder()
                .add(disjunct, BooleanClause.Occur.MUST)
                .add(new TermQuery(new Term(textfield, "good")), BooleanClause.Occur.MUST)
                .build();


        monitor = new Monitor((queryString, metadata) -> outerConjunct, new MatchAllPresearcher());
        monitor.update(new MonitorQuery("1", ""));

        InputDocument doc = buildDoc("1", "now is the time for all good men");
        Matches<HighlightsMatch> matches = monitor.match(doc, HighlightingMatcher.FACTORY);

        assertThat(matches)
            .matchesQuery("1", "1")
                .withHitCount(2)
                .inField(textfield)
                    .withHit(new HighlightsMatch.Hit(0, 0, 0, 3))
                    .withHit(new HighlightsMatch.Hit(6, 24, 6, 28));
    }

    @Test
    public void testMinShouldMatchQuery() throws Exception {

        final Query minq = new BooleanQuery.Builder()
                .add(new TermQuery(new Term(textfield, "x")), BooleanClause.Occur.SHOULD)
                .add(new TermQuery(new Term(textfield, "y")), BooleanClause.Occur.SHOULD)
                .add(new TermQuery(new Term(textfield, "z")), BooleanClause.Occur.SHOULD)
                .setMinimumNumberShouldMatch(2)
                .build();

        final Query bq = new BooleanQuery.Builder()
                .add(new TermQuery(new Term(textfield, "a")), BooleanClause.Occur.MUST)
                .add(new TermQuery(new Term(textfield, "b")), BooleanClause.Occur.MUST)
                .add(minq, BooleanClause.Occur.SHOULD)
                .build();

        monitor = new Monitor((queryString, metadata) -> bq, new MatchAllPresearcher());
        monitor.update(new MonitorQuery("1", ""));

        InputDocument doc = buildDoc("1", "a b x");
        Matches<HighlightsMatch> matches = monitor.match(doc, HighlightingMatcher.FACTORY);

        assertThat(matches).matchesQuery("1", "1")
            .withHitCount(2)
            .inField(textfield)
                .withHit(new HighlightsMatch.Hit(0, 0, 0, 1))
                .withHit(new HighlightsMatch.Hit(1, 2, 1, 3));
    }

    @Test
    public void testComplexPhraseQueryParser() throws IOException, UpdateException {

        ComplexPhraseQueryParser cpqp = new ComplexPhraseQueryParser(textfield, new StandardAnalyzer());
        MonitorQueryParser cqp = (queryString, metadata) -> cpqp.parse(queryString);
        monitor = new Monitor(cqp, new MatchAllPresearcher());
        monitor.update(new MonitorQuery("1", "\"x b\""));

        InputDocument doc = buildDoc("1", "x b c");
        Matches<HighlightsMatch> matches = monitor.match(doc, HighlightingMatcher.FACTORY);

        assertThat(matches).matchesQuery("1", "1")
                .withHitCount(2)
                .inField(textfield);

    }

    @Test
    public void highlightBatches() throws Exception {
        String query = "\"cell biology\"";
        String matching_document = "the cell biology count";

        monitor.update(new MonitorQuery("query0", "non matching query"));
        monitor.update(new MonitorQuery("query1", query));
        monitor.update(new MonitorQuery("query2", "biology"));

        DocumentBatch batch = DocumentBatch.of(
                InputDocument.builder("doc1").addField(textfield, matching_document, WHITESPACE).build(),
                InputDocument.builder("doc2").addField(textfield, "nope", WHITESPACE).build(),
                InputDocument.builder("doc3").addField(textfield, "biology text", WHITESPACE).build()
        );

        Matches<HighlightsMatch> matches = monitor.match(batch, HighlightingMatcher.FACTORY);

        assertThat(matches)
                .hasMatchCount("doc1", 2)
                .hasMatchCount("doc2", 0)
                .hasMatchCount("doc3", 1)
                .matchesQuery("query1", "doc1")
                    .inField(textfield)
                        .withHit(new HighlightsMatch.Hit(1, 4, 1, 8))
                        .withHit(new HighlightsMatch.Hit(2, 9, 2, 16))
                .matchesQuery("query2", "doc3")
                    .inField(textfield)
                        .withHit(new HighlightsMatch.Hit(0, 0, 0, 7));
    }

}
