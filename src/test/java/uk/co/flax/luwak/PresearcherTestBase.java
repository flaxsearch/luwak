package uk.co.flax.luwak;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.util.Version;
import org.junit.Before;

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

    protected final Monitor monitor = new Monitor();

    protected Presearcher presearcher;

    @Before
    public void setUp() {
        monitor.reset();
        presearcher = createPresearcher();
    }

    protected abstract Presearcher createPresearcher();

    public static final String TEXTFIELD = "text";

    public static final Analyzer WHITESPACE = new WhitespaceAnalyzer(Version.LUCENE_50);

}
