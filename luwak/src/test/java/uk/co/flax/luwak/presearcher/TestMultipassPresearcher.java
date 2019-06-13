package uk.co.flax.luwak.presearcher;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import uk.co.flax.luwak.DocumentBatch;
import uk.co.flax.luwak.InputDocument;
import uk.co.flax.luwak.Matches;
import uk.co.flax.luwak.Monitor;
import uk.co.flax.luwak.MonitorQuery;
import uk.co.flax.luwak.Presearcher;
import uk.co.flax.luwak.QueryMatch;
import uk.co.flax.luwak.QueryTermFilter;
import uk.co.flax.luwak.UpdateException;
import uk.co.flax.luwak.matchers.SimpleMatcher;
import uk.co.flax.luwak.queryparsers.LuceneQueryParser;

import java.io.IOException;

import static uk.co.flax.luwak.assertions.MatchesAssert.assertThat;

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
    public void testSimpleBoolean() throws IOException, UpdateException {

        monitor.update(new MonitorQuery("1", "field:\"hello world\""),
                       new MonitorQuery("2", "field:world"),
                       new MonitorQuery("3", "field:\"hello there world\""),
                       new MonitorQuery("4", "field:\"this and that\""));

        Matches<QueryMatch> matches = monitor.match(buildDoc("doc1", "field", "hello world and goodbye"),
                                                        SimpleMatcher.FACTORY);
        assertThat(matches)
                .hasQueriesRunCount(2)
                .matchesQuery("1", "doc1");

    }

    @Test
    public void testComplexBoolean() throws IOException, UpdateException {

        monitor.update(new MonitorQuery("1", "field:(+foo +bar +(badger cormorant))"));

        assertThat(monitor.match(buildDoc("doc1", "field", "a badger walked into a bar"), SimpleMatcher.FACTORY))
                .hasMatchCount("doc1", 0)
                .hasQueriesRunCount(0);

        assertThat(monitor.match(buildDoc("doc2", "field", "foo badger cormorant"), SimpleMatcher.FACTORY))
                .hasMatchCount("doc2", 0)
                .hasQueriesRunCount(0);

        assertThat(monitor.match(buildDoc("doc3", "field", "bar badger foo"), SimpleMatcher.FACTORY))
                .hasMatchCount("doc3", 1);

    }

    @Test
    public void testMultipleFields() throws IOException, UpdateException {

        monitor.update(new MonitorQuery("1", "field1:(foo OR bar) AND field2:cormorant"));
        InputDocument doc = InputDocument.builder("doc1")
                .addField("field1", "a badger walked into a bar", WHITESPACE)
                .addField("field2", "cormorant", WHITESPACE)
                .build();

        assertThat(monitor.match(doc, SimpleMatcher.FACTORY))
                .hasQueriesRunCount(1)
                .hasMatchCount("doc1", 1);
    }

    @Test
    public void testQueryBuilder() throws IOException, UpdateException {

        IndexWriterConfig iwc = new IndexWriterConfig(new KeywordAnalyzer());
        Presearcher presearcher = createPresearcher();

        Directory dir = new RAMDirectory();
        IndexWriter writer = new IndexWriter(dir, iwc);
        try (Monitor monitor = new Monitor(new LuceneQueryParser("f"), presearcher, writer)) {

            monitor.update(new MonitorQuery("1", "f:test"));

            try (IndexReader reader = DirectoryReader.open(writer, false, false)) {

                InputDocument doc = InputDocument.builder("doc1")
                        .addField("f", "this is a test document", WHITESPACE).build();
                DocumentBatch docs = DocumentBatch.of(doc);

                BooleanQuery q = (BooleanQuery) presearcher.buildQuery(docs.getIndexReader(), new QueryTermFilter(reader));
                BooleanQuery expected = new BooleanQuery.Builder()
                        .add(should(new BooleanQuery.Builder()
                                .add(must(new BooleanQuery.Builder().add(should(new TermInSetQuery("f_0", new BytesRef("test")))).build()))
                                .add(must(new BooleanQuery.Builder().add(should(new TermInSetQuery("f_1", new BytesRef("test")))).build()))
                                .add(must(new BooleanQuery.Builder().add(should(new TermInSetQuery("f_2", new BytesRef("test")))).build()))
                                .add(must(new BooleanQuery.Builder().add(should(new TermInSetQuery("f_3", new BytesRef("test")))).build()))
                                .build()))
                        .add(should(new TermQuery(new Term("__anytokenfield", "__ANYTOKEN__"))))
                        .build();

                Assertions.assertThat(q).isEqualTo(expected);
            }

        }

    }

}
