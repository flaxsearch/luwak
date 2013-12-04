package uk.co.flax.luwak;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermQuery;
import org.junit.Test;
import uk.co.flax.luwak.impl.TermFilteredPresearcher;

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
public class TestTermPresearcher extends PresearcherTestBase {

    @Test
    public void filtersOnTermQueries() {

        MonitorQuery query1
                = new MonitorQuery("1", new TermQuery(new Term(TEXTFIELD, "furble")));
        MonitorQuery query2
                = new MonitorQuery("2", new TermQuery(new Term(TEXTFIELD, "document")));
        monitor.update(query1, query2);

        InputDocument doc = InputDocument.builder("doc1")
                .addField(TEXTFIELD, "this is a test document", WHITESPACE)
                .build();

        assertThat(monitor.match(doc))
                .hasMatchCount(1)
                .hasQueriesRunCount(1);

    }

    @Test
    public void ignoresTermsOnNotQueries() {

        BooleanQuery bq = new BooleanQuery();
        bq.add(new TermQuery(new Term(TEXTFIELD, "document")), BooleanClause.Occur.SHOULD);
        bq.add(new TermQuery(new Term(TEXTFIELD, "test")), BooleanClause.Occur.MUST_NOT);

        monitor.update(new MonitorQuery("1", bq));

        InputDocument doc1 = InputDocument.builder("doc1")
                .addField(TEXTFIELD, "this is a test document", WHITESPACE)
                .build();

        assertThat(monitor.match(doc1))
                .hasMatchCount(0)
                .hasQueriesRunCount(1);

        InputDocument doc2 = InputDocument.builder("doc2")
                .addField(TEXTFIELD, "weeble sclup test", WHITESPACE)
                .build();

        assertThat(monitor.match(doc2))
                .hasMatchCount(0)
                .hasQueriesRunCount(0);
    }

    @Test
    public void matchesAnyQueries() {

        monitor.update(new MonitorQuery("1", new RegexpQuery(new Term(TEXTFIELD, "hell?"))));

        InputDocument doc = InputDocument.builder("doc1")
                .addField(TEXTFIELD, "whatever", WHITESPACE)
                .build();

        assertThat(monitor.match(doc))
                .hasMatchCount(0)
                .hasQueriesRunCount(1);

    }

    @Override
    protected Presearcher createPresearcher() {
        return new TermFilteredPresearcher();
    }
}
