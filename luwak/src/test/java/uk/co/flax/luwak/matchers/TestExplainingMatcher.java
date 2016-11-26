package uk.co.flax.luwak.matchers;

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

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.search.Explanation;
import org.junit.Test;
import uk.co.flax.luwak.*;
import uk.co.flax.luwak.presearcher.MatchAllPresearcher;
import uk.co.flax.luwak.queryparsers.LuceneQueryParser;

import static org.assertj.core.api.Assertions.assertThat;

public class TestExplainingMatcher {

    @Test
    public void testExplainingMatcher() throws IOException, UpdateException {

        try (Monitor monitor = new Monitor(new LuceneQueryParser("field"), new MatchAllPresearcher())) {
            monitor.update(new MonitorQuery("1", "test"), new MonitorQuery("2", "wibble"));

            InputDocument doc1 = InputDocument.builder("doc1").addField("field", "test", new StandardAnalyzer()).build();

            Matches<ExplainingMatch> matches = monitor.match(doc1, ExplainingMatcher.FACTORY);
            assertThat(matches.matches("1", "doc1")).isNotNull();
            assertThat(matches.matches("1", "doc1").getExplanation()).isNotNull();
        }
    }

    @Test
    public void testHashcodeAndEquals() {

        ExplainingMatch m1 = new ExplainingMatch("1", "1", Explanation.match(0.1f, "an explanation"));
        ExplainingMatch m2 = new ExplainingMatch("1", "2", Explanation.match(0.1f, "an explanation"));
        ExplainingMatch m3 = new ExplainingMatch("1", "1", Explanation.match(0.1f, "another explanation"));
        ExplainingMatch m4 = new ExplainingMatch("1", "1", Explanation.match(0.1f, "an explanation"));

        assertThat(m1).isEqualTo(m4);
        assertThat(m1.hashCode()).isEqualTo(m4.hashCode());
        assertThat(m1).isNotEqualTo(m2);
        assertThat(m1.hashCode()).isNotEqualTo(m2.hashCode());
        assertThat(m1).isNotEqualTo(m3);
        assertThat(m3).isNotEqualTo(m4);

    }
}
