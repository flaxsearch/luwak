package uk.co.flax.luwak;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.search.BooleanQuery;
import org.junit.Test;
import uk.co.flax.luwak.matchers.SimpleMatcher;
import uk.co.flax.luwak.presearcher.TermFilteredPresearcher;
import uk.co.flax.luwak.queryparsers.LuceneQueryParser;

import static org.assertj.core.api.Assertions.assertThat;

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
public class TestTermsEnumFilter {

    public static final Analyzer ANALYZER = new WhitespaceAnalyzer();

    @Test
    public void testOnlyExistingTermsAreUsedInQuery() throws IOException {

        Monitor monitor = new Monitor(new LuceneQueryParser("f"), new TermFilteredPresearcher());
        monitor.update(new MonitorQuery("1", "f:should"), new MonitorQuery("2", "+text:hello +text:world"));

        InputDocument doc = InputDocument.builder("doc")
                .addField("text", "this is a document about the world saying hello", ANALYZER)
                .addField("title", "but this text should be ignored", ANALYZER)
                .build();

        BooleanQuery query = (BooleanQuery) monitor.buildQuery(doc);

        assertThat(query.clauses()).hasSize(2);     // text:world __anytokenfield:__ANYTOKEN__
        assertThat(monitor.match(doc, SimpleMatcher.FACTORY).getMatchCount()).isEqualTo(1);

        monitor.close();
    }

}
