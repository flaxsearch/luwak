package uk.co.flax.luwak.presearcher;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.*;
import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRefHash;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import uk.co.flax.luwak.*;
import uk.co.flax.luwak.matchers.SimpleMatcher;
import uk.co.flax.luwak.queryparsers.LuceneQueryParser;
import uk.co.flax.luwak.termextractor.querytree.QueryTree;

import static uk.co.flax.luwak.assertions.MatchesAssert.assertThat;

/*
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

public class TestTermPresearcher extends PresearcherTestBase {

    @Test
    public void filtersOnTermQueries() throws IOException, UpdateException {

        MonitorQuery query1
                = new MonitorQuery("1", "furble");
        MonitorQuery query2
                = new MonitorQuery("2", "document");
        MonitorQuery query3 = new MonitorQuery("3", "\"a document\"");  // will be selected but not match
        monitor.update(query1, query2, query3);

        Matches<QueryMatch> matcher = monitor.match(buildDoc("doc1", TEXTFIELD, "this is a test document"), SimpleMatcher.FACTORY);
        assertThat(matcher)
                .hasMatchCount("doc1", 1)
                .selectedQueries("2", "3")
                .matchesQuery("2", "doc1")
                .hasQueriesRunCount(2);

    }

    @Test
    public void ignoresTermsOnNotQueries() throws IOException, UpdateException {

        monitor.update(new MonitorQuery("1", "document -test"));

        assertThat(monitor.match(buildDoc("doc1", TEXTFIELD, "this is a test document"), SimpleMatcher.FACTORY))
                .hasMatchCount("doc1", 0)
                .hasQueriesRunCount(1);

        assertThat(monitor.match(buildDoc("doc2", TEXTFIELD, "weeble sclup test"), SimpleMatcher.FACTORY))
                .hasMatchCount("doc2", 0)
                .hasQueriesRunCount(0);
    }

    @Test
    public void matchesAnyQueries() throws IOException, UpdateException {

        monitor.update(new MonitorQuery("1", "/hell./"));

        assertThat(monitor.match(buildDoc("doc1", TEXTFIELD, "hello"), SimpleMatcher.FACTORY))
                .hasMatchCount("doc1", 1)
                .hasQueriesRunCount(1);

    }

    @Override
    protected Presearcher createPresearcher() {
        return new TermFilteredPresearcher();
    }

    @Test
    public void testAnyTermsAreCorrectlyAnalyzed() {

        TermFilteredPresearcher presearcher = new TermFilteredPresearcher();
        QueryTree qt = presearcher.extractor.buildTree(new MatchAllDocsQuery());

        Map<String, BytesRefHash> extractedTerms = presearcher.collectTerms(qt);

        Assertions.assertThat(extractedTerms.size()).isEqualTo(1);

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

                DocumentBatch batch = DocumentBatch.of(
                        InputDocument.builder("doc1").addField("f", "this is a test document", new StandardAnalyzer()).build()
                );

                BooleanQuery q = (BooleanQuery) presearcher.buildQuery(batch.getIndexReader(), new QueryTermFilter(reader));
                BooleanQuery expected = new BooleanQuery.Builder()
                        .add(should(new TermsQuery(new Term("f", "test"))))
                        .add(should(new TermQuery(new Term("__anytokenfield", "__ANYTOKEN__"))))
                        .build();

                Assertions.assertThat(q).isEqualTo(expected);

            }

        }

    }
}
