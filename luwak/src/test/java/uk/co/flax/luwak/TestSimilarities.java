package uk.co.flax.luwak;

import java.io.IOException;

import com.google.common.collect.Iterables;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.junit.Test;
import uk.co.flax.luwak.matchers.ScoringMatch;
import uk.co.flax.luwak.matchers.ScoringMatcher;
import uk.co.flax.luwak.presearcher.MatchAllPresearcher;
import uk.co.flax.luwak.queryparsers.LuceneQueryParser;

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

public class TestSimilarities {

    public static final Analyzer ANALYZER = new WhitespaceAnalyzer();

    @Test
    public void testNonStandardSimilarity() throws IOException, UpdateException {

        try (Monitor monitor = new Monitor(new LuceneQueryParser("field"), new MatchAllPresearcher()))
        {
            monitor.update(new MonitorQuery("1", "test"));

            Similarity similarity = new ClassicSimilarity() {
                @Override
                public float tf(float freq) {
                    return 1000f;
                }
            };

            InputDocument doc = InputDocument.builder("doc")
                    .addField("field", "this is a test", new StandardAnalyzer()).build();

            DocumentBatch batch = new DocumentBatch.Builder()
                    .add(doc)
                    .setSimilarity(similarity)
                    .build();

            DocumentBatch standardBatch = new DocumentBatch.Builder()
                    .add(doc)
                    .setSimilarity(new ClassicSimilarity())
                    .build();

            Matches<ScoringMatch> standard = monitor.match(standardBatch, ScoringMatcher.FACTORY);
            Matches<ScoringMatch> withSim = monitor.match(batch, ScoringMatcher.FACTORY);

            assertThat(Iterables.getFirst(standard.getMatches("doc"), null).getScore())
                    .isEqualTo(Iterables.getFirst(withSim.getMatches("doc"), null).getScore() / 1000);
        }
    }
}
