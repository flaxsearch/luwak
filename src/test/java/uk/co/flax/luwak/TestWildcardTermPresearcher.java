package uk.co.flax.luwak;

import org.junit.Test;
import uk.co.flax.luwak.impl.WildcardNGramPresearcher;
import uk.co.flax.luwak.matchers.SimpleMatcher;

import java.io.IOException;

import static uk.co.flax.luwak.util.MatchesAssert.assertThat;

/**
 * Copyright (c) 2013 Lemur Consulting Ltd.
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

public class TestWildcardTermPresearcher extends PresearcherTestBase {

    @Test
    public void filtersWildcards() throws IOException {

        monitor.update(new MonitorQuery("1", "/hell.*/"));

        InputDocument doc1 = InputDocument.builder("doc1")
                .addField(TEXTFIELD, "well hello there", WHITESPACE)
                .build();

        assertThat(monitor.match(doc1, SimpleMatcher.factory()))
                .hasMatchCount(1);

    }

    @Override
    protected Presearcher createPresearcher() {
        return new WildcardNGramPresearcher();
    }
}
