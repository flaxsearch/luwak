package uk.co.flax.luwak;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Bits;
import org.junit.Test;

import uk.co.flax.luwak.matchers.SimpleMatcher;
import uk.co.flax.luwak.presearcher.MatchAllPresearcher;
import static org.assertj.core.api.Assertions.assertThat;

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

public class TestSlowLog {

    public static class SlowQueryParser implements MonitorQueryParser {

        final long delay;

        public SlowQueryParser(long delay) {
            this.delay = delay;
        }

        @Override
        public Query parse(String queryString, Map<String, String> metadata) throws Exception {
            if (queryString.equals("slow")) {
                return new Query() {
                    @Override
                    public String toString(String s) {
                        return "";
                    }

                    @Override
                    public Weight createWeight(IndexSearcher searcher, boolean needsScores, float boost) {
                        try {
                            Thread.sleep(delay);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        return new ConstantScoreWeight(this, boost) {
                            @Override
                            public final Scorer scorer(LeafReaderContext context) throws IOException {
                              final Bits matchingDocs = getMatchingDocs(context);
                              if (matchingDocs == null || matchingDocs instanceof Bits.MatchNoBits) {
                                return null;
                              }
                              final DocIdSetIterator approximation = DocIdSetIterator.all(context.reader().maxDoc());
                              final TwoPhaseIterator twoPhase = new TwoPhaseIterator(approximation) {

                                @Override
                                public boolean matches() throws IOException {
                                  final int doc = approximation.docID();

                                  return matchingDocs.get(doc);
                                }

                                @Override
                                public float matchCost() {
                                  return 10; // TODO: use some cost of matchingDocs
                                }
                              };

                              return new ConstantScoreScorer(this, score(), twoPhase);
                            }

                            protected Bits getMatchingDocs(LeafReaderContext context) throws IOException {
                                return new Bits.MatchAllBits(context.reader().maxDoc());
                            }

                            public String toString() {
                                return "weight(MatchAllDocs)";
                            }

                            @Override
                            public boolean isCacheable(LeafReaderContext ctx) {
                                // TODO Auto-generated method stub
                                return false;
                            }
                        };
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
            return new MatchAllDocsQuery();
        }
    }

    @Test
    public void testSlowLog() throws IOException, UpdateException {

        try (Monitor monitor = new Monitor(new SlowQueryParser(250), new MatchAllPresearcher())) {
            monitor.update(new MonitorQuery("1", "slow"), new MonitorQuery("2", "fast"), new MonitorQuery("3", "slow"));

            InputDocument doc1 = InputDocument.builder("doc1").build();

            Matches<QueryMatch> matches = monitor.match(doc1, SimpleMatcher.FACTORY);
            System.out.println(matches.getSlowLog());
            assertThat(matches.getSlowLog().toString())
                    .contains("1 [")
                    .contains("3 [")
                    .doesNotContain("2 [");

            monitor.setSlowLogLimit(1);
            assertThat(monitor.match(doc1, SimpleMatcher.FACTORY).getSlowLog().toString())
                    .contains("1 [")
                    .contains("2 [")
                    .contains("3 [");

            monitor.setSlowLogLimit(2000000000000l);
            assertThat(monitor.match(doc1, SimpleMatcher.FACTORY).getSlowLog())
                    .isEmpty();
        }
    }
}
