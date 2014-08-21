package uk.co.flax.luwak.presearcher;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.junit.Before;
import uk.co.flax.luwak.Monitor;
import uk.co.flax.luwak.Presearcher;
import uk.co.flax.luwak.parsers.LuceneQueryCache;

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
        monitor = new Monitor(new LuceneQueryCache(TEXTFIELD, WHITESPACE), presearcher);
    }

    protected abstract Presearcher createPresearcher();

    public static final String TEXTFIELD = "text";

    public static final Analyzer WHITESPACE = new WhitespaceAnalyzer();

}
