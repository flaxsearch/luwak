package uk.co.flax.luwak.presearcher;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.junit.Before;
import org.junit.Test;
import uk.co.flax.luwak.*;
import uk.co.flax.luwak.matchers.SimpleMatcher;
import uk.co.flax.luwak.queryparsers.LuceneQueryParser;

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

public abstract class PresearcherTestBase {

    protected Monitor monitor;

    protected Presearcher presearcher;

    @Before
    public void setUp() throws IOException {
        presearcher = createPresearcher();
        monitor = new Monitor(new LuceneQueryParser(TEXTFIELD, WHITESPACE), presearcher);
    }

    protected abstract Presearcher createPresearcher();

    public static final String TEXTFIELD = "text";

    public static final Analyzer WHITESPACE = new WhitespaceAnalyzer(Constants.VERSION);

    @Test
    public void testNullFieldHandling() throws IOException {

        monitor.update(new MonitorQuery("1", "field_1:test"));

        InputDocument doc = InputDocument.builder("doc1").addField("field_2", "test", WHITESPACE).build();

        assertThat(monitor.match(doc, SimpleMatcher.FACTORY))
                .hasMatchCount(0);

    }

}
