package uk.co.flax.luwak;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.Version;
import org.junit.Test;
import uk.co.flax.luwak.impl.MatchAllDocsQueryFactory;
import uk.co.flax.luwak.impl.SingleFieldInputDocument;
import uk.co.flax.luwak.impl.TermFilteredMonitorQuery;
import uk.co.flax.luwak.impl.TermFilteredPresearcherQueryFactory;

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
public class TestPresearcher {

    public static final String TEXTFIELD = "field";
    public static final String FILTERFIELD = "filter";

    static class AnalyzedInputDocument extends SingleFieldInputDocument {

        public AnalyzedInputDocument(String id, String text, PresearcherQueryFactory qf) {
            super(id, TEXTFIELD, text, new WhitespaceAnalyzer(Version.LUCENE_50), qf);
        }

        public AnalyzedInputDocument(String id, String text) {
            this(id, text, new MatchAllDocsQueryFactory());
        }

    }

    static class FilterTermPresearcherQueryFactory implements PresearcherQueryFactory {

        final String filter;

        FilterTermPresearcherQueryFactory(String filter) {
            this.filter = filter;
        }

        @Override
        public Query buildQuery(InputDocument doc) {
            return new TermQuery(new Term(FILTERFIELD, filter));
        }
    }

    static class FilterFieldMonitorQuery extends MonitorQuery {

        public final String filter;

        public FilterFieldMonitorQuery(String id, Query query, String filter) {
            super(id, query);
            this.filter = filter;
        }

        @Override
        protected void addFields(Document doc) {
            doc.add(new StringField(FILTERFIELD, filter, Field.Store.NO));
        }
    }

    @Test
    public void presearcherFiltersOutQueries() {

        FilterFieldMonitorQuery query1
                = new FilterFieldMonitorQuery("1", new TermQuery(new Term(TEXTFIELD, "this")), "pub1");
        FilterFieldMonitorQuery query2
                = new FilterFieldMonitorQuery("2", new TermQuery(new Term(TEXTFIELD, "document")), "pub2");
        Monitor monitor = new Monitor(query1, query2);

        InputDocument doc = new AnalyzedInputDocument("doc1", "this is a test document",
                new FilterTermPresearcherQueryFactory("pub1"));
        assertThat(monitor.match(doc))
                        .hasMatchCount(1)
                        .matchesQuery("1");

        InputDocument doc2 = new AnalyzedInputDocument("doc2", "this is a test document");
        assertThat(monitor.match(doc2))
                        .hasMatchCount(2);

    }

    @Test
    public void filtersOnTermQueries() {

        TermFilteredMonitorQuery query1
                = new TermFilteredMonitorQuery("1", new TermQuery(new Term(TEXTFIELD, "furble")));
        TermFilteredMonitorQuery query2
                = new TermFilteredMonitorQuery("2", new TermQuery(new Term(TEXTFIELD, "document")));
        Monitor monitor = new Monitor(query1, query2);

        InputDocument doc = new AnalyzedInputDocument("doc1", "this is a test document",
                new TermFilteredPresearcherQueryFactory());

        assertThat(monitor.match(doc))
                        .hasMatchCount(1)
                        .hasQueriesRunCount(1);

    }

}
