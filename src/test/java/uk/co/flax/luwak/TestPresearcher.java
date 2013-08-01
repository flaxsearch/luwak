package uk.co.flax.luwak;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.Version;
import org.junit.Test;

import static org.fest.assertions.api.Assertions.assertThat;

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

        public AnalyzedInputDocument(String id, String text) {
            super(id, TEXTFIELD, text, new WhitespaceAnalyzer(Version.LUCENE_50));
        }

        @Override
        public Query getPresearcherQuery() {
            return new MatchAllDocsQuery();
        }
    }

    static class PresearcherInputDocument extends AnalyzedInputDocument {

        public final String filter;

        public PresearcherInputDocument(String id, String text, String filter) {
            super(id, text);
            this.filter = filter;
        }

        @Override
        public Query getPresearcherQuery() {
            return new TermQuery(new Term(FILTERFIELD, filter));
        }
    }

    static class PresearcherMonitorQuery extends MonitorQuery {

        public final String filter;

        public PresearcherMonitorQuery(String id, Query query, String filter) {
            super(id, query);
            this.filter = filter;
        }

        @Override
        protected void addFields() {
            indexDoc.add(new StringField(FILTERFIELD, filter, Field.Store.NO));
        }
    }

    @Test
    public void presearcherFiltersOutQueries() {

        PresearcherMonitorQuery query1
                = new PresearcherMonitorQuery("1", new TermQuery(new Term(TEXTFIELD, "this")), "pub1");
        PresearcherMonitorQuery query2
                = new PresearcherMonitorQuery("2", new TermQuery(new Term(TEXTFIELD, "document")), "pub2");
        Monitor monitor = new Monitor(query1, query2);

        InputDocument doc = new PresearcherInputDocument("doc1", "this is a test document", "pub1");
        MatchResponse response = monitor.match(doc);

        assertThat(response.matches()).hasSize(1);
        assertThat(response.matches().get(0).getQueryId()).isEqualTo("1");

        InputDocument doc2 = new AnalyzedInputDocument("doc2", "this is a test document");
        MatchResponse response2 = monitor.match(doc2);
        assertThat(response2.matches()).hasSize(2);

    }

}
