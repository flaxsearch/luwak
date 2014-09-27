package uk.co.flax.luwak.presearcher;

import java.io.IOException;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.junit.Test;
import uk.co.flax.luwak.*;
import uk.co.flax.luwak.matchers.SimpleMatcher;
import uk.co.flax.luwak.queryparsers.LuceneQueryParser;

import static org.fest.assertions.api.Assertions.assertThat;

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

public class TestPresearcherMatchCollector {

    public static final String TEXTFIELD = "f";

    @Test
    public void testMatchCollectorShowMatches() throws IOException {

        Monitor monitor = new Monitor(new LuceneQueryParser(TEXTFIELD), new TermFilteredPresearcher());
        monitor.update(new MonitorQuery("1", "test"));
        monitor.update(new MonitorQuery("2", "foo bar -baz"));
        monitor.update(new MonitorQuery("3", "foo -test"));
        monitor.update(new MonitorQuery("4", "baz"));

        InputDocument doc = InputDocument.builder("doc1")
                .addField(TEXTFIELD, "this is a foo test", new WhitespaceAnalyzer(Constants.VERSION))
                .build();

        PresearcherMatches<QueryMatch> matches = monitor.debug(doc, SimpleMatcher.FACTORY);

        assertThat(matches.match("1")).isNotNull();
        assertThat(matches.match("1").presearcherMatches).isEqualTo(" f:test");
        assertThat(matches.match("1").queryMatch)
                .isNotNull()
                .isInstanceOf(QueryMatch.class);

        assertThat(matches.match("2")).isNotNull();
        assertThat(matches.match("2").presearcherMatches).isEqualTo(" f:foo");

        assertThat(matches.match("3")).isNotNull();
        assertThat(matches.match("3").presearcherMatches).isEqualTo(" f:foo");
        assertThat(matches.match("3").queryMatch).isNull();

        assertThat(matches.match("4")).isNull();

    }

}
