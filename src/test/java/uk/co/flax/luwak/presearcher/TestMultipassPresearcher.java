package uk.co.flax.luwak.presearcher;

import java.io.IOException;

import org.junit.Test;
import uk.co.flax.luwak.InputDocument;
import uk.co.flax.luwak.MonitorQuery;
import uk.co.flax.luwak.Presearcher;
import uk.co.flax.luwak.matchers.SimpleMatcher;

import static uk.co.flax.luwak.util.MatchesAssert.assertThat;

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

public class TestMultipassPresearcher extends PresearcherTestBase {

    @Override
    protected Presearcher createPresearcher() {
        return new MultipassTermFilteredPresearcher(4);
    }

    @Test
    public void testSimpleBoolean() throws IOException {

        monitor.update(new MonitorQuery("1", "field:\"hello world\""),
                       new MonitorQuery("2", "field:world"),
                       new MonitorQuery("3", "field:\"hello there world\""),
                       new MonitorQuery("4", "field:\"this and that\""));

        InputDocument doc = InputDocument.builder("doc1")
                .addField("field", "hello world and goodbye", WHITESPACE)
                .build();

        SimpleMatcher matches = monitor.match(doc, SimpleMatcher.FACTORY);
        assertThat(matches)
                .hasQueriesRunCount(2)
                .matchesQuery("1");

    }
}
