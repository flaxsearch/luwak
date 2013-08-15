package uk.co.flax.luwak;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.Version;
import org.junit.Test;
import uk.co.flax.luwak.impl.SingleFieldInputDocument;

import java.util.List;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.fest.assertions.api.Assertions.fail;

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

public class TestMonitor {

    static class BasicInputDocument extends SingleFieldInputDocument {

        BasicInputDocument(String id, String text) {
            super(id, textfield, text, new WhitespaceAnalyzer(Version.LUCENE_50));
        }

    }

    static final String textfield = "textfield";

    @Test
    public void singleTermQueryMatchesSingleDocument() {

        String document = "This is a test document";

        Query query = new TermQuery(new Term(textfield, "test"));
        MonitorQuery mq = new MonitorQuery("query1", query);

        InputDocument doc = new BasicInputDocument("doc1", document);

        Monitor monitor = new Monitor(mq);
        DocumentMatches response = monitor.match(doc);

        assertThat(response.docId()).isEqualTo("doc1");
        assertThat(response.matches()).hasSize(1);

        QueryMatch match = response.matches().get(0);
        assertThat(match.getQueryId()).isEqualTo("query1");

        List<QueryMatch.Hit> hits = match.getHits();
        assertThat(hits).hasSize(1);

        QueryMatch.Hit hit = hits.get(0);
        assertThat(hit.startPosition).isEqualTo(3);
        assertThat(hit.endPosition).isEqualTo(3);

    }

    @Test(expected = IllegalStateException.class)
    public void monitorWithNoQueriesThrowsException() {
        Monitor monitor = new Monitor();
        InputDocument doc = new BasicInputDocument("doc1", "test");
        monitor.match(doc);
        fail("Monitor with no queries should have thrown an IllegalStateException");
    }

}
