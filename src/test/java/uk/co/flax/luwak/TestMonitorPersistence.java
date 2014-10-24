package uk.co.flax.luwak;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.store.MMapDirectory;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import uk.co.flax.luwak.matchers.SimpleMatcher;
import uk.co.flax.luwak.presearcher.TermFilteredPresearcher;
import uk.co.flax.luwak.queryparsers.LuceneQueryParser;
import uk.co.flax.luwak.util.FileUtils;

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

public class TestMonitorPersistence {

    private Path indexDirectory;

    @Before
    public void setup() throws IOException {
        indexDirectory = Files.createTempDirectory("persistent-index");
    }

    @Test
    public void testCacheIsRepopulated() throws IOException {

        Monitor monitor = new Monitor(new LuceneQueryParser("f"), new TermFilteredPresearcher(),
                                        new MMapDirectory(indexDirectory));
        monitor.update(new MonitorQuery("1", "test"),
                       new MonitorQuery("2", "test", "test"),
                       new MonitorQuery("3", "test", ImmutableMap.of("language", "en")),
                       new MonitorQuery("4", "test", "test", ImmutableMap.of("language", "en", "wibble", "quack")));

        InputDocument doc = InputDocument.builder("doc1").addField("f", "test", new KeywordAnalyzer()).build();

        assertThat(monitor.match(doc, SimpleMatcher.FACTORY))
                .hasMatchCount(4);

        monitor.close();

        Monitor monitor2 = new Monitor(new LuceneQueryParser("f"), new TermFilteredPresearcher(),
                                        new MMapDirectory(indexDirectory));

        Assertions.assertThat(monitor2.getQueryCount()).isEqualTo(4);
        assertThat(monitor2.match(doc, SimpleMatcher.FACTORY)).hasMatchCount(4);

    }

    @After
    public void teardown() throws IOException {
        FileUtils.deleteDirectory(indexDirectory);
    }

}
