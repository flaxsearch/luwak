package uk.co.flax.luwak.matchers;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.junit.Test;
import uk.co.flax.luwak.*;
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

public class TestQueryCacheingMatcher {

    public static final Analyzer ANALYZER = new WhitespaceAnalyzer();

    @Test
    public void testQueryCacheingMatcher() throws IOException {

        Monitor monitor = new Monitor(new LuceneQueryParser("field"), new MatchAllPresearcher());
        monitor.update(new MonitorQuery("1", "test"), new MonitorQuery("2", "wibble"));

        InputDocument doc1 = InputDocument.builder("doc1").addField("field", "test", ANALYZER).build();

        Matches<QueryCacheingMatch<QueryMatch>> match = monitor.match(doc1, QueryCacheingMatcher.factory(SimpleMatcher.FACTORY));
        assertThat(match.matches("1")).isNotNull();
        assertThat(match.matches("2")).isNull();
        assertThat(match.matches("1").query).isEqualTo(new TermQuery(new Term("field", "test")));

        Matches<QueryCacheingMatch<ExplainingMatch>> match2 = monitor.match(doc1, QueryCacheingMatcher.factory(ExplainingMatcher.FACTORY));
        assertThat(match2.matches("1")).isNotNull();
        assertThat(match2.matches("2")).isNull();
        assertThat(match2.matches("1").query).isEqualTo(new TermQuery(new Term("field", "test")));
        assertThat(match2.matches("1").wrappedMatch.getExplanation().isMatch()).isTrue();

    }

}
