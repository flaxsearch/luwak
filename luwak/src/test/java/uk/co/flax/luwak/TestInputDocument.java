package uk.co.flax.luwak;

/*
 *   Copyright (c) 2015 Lemur Consulting Ltd.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.Explanation;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import uk.co.flax.luwak.matchers.ExplainingMatch;
import uk.co.flax.luwak.matchers.ExplainingMatcher;
import uk.co.flax.luwak.presearcher.MatchAllPresearcher;
import uk.co.flax.luwak.queryparsers.LuceneQueryParser;

import static org.assertj.core.api.Assertions.assertThat;

public class TestInputDocument {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Test
    public void testCannotAddReservedFieldName() {
        expected.expect(IllegalArgumentException.class);
        expected.expectMessage("reserved");

        InputDocument.builder("id").addField(InputDocument.ID_FIELD, "test", new StandardAnalyzer()).build();
    }

    @Test
    public void testCannotAddReservedFieldObject() {
        expected.expect(IllegalArgumentException.class);
        expected.expectMessage("reserved");

        InputDocument.builder("id").addField(new StringField(InputDocument.ID_FIELD, "", Field.Store.YES)).build();
    }

    @Test
    public void testOmitNorms() throws Exception {
        FieldType type = new FieldType();
        type.setOmitNorms(true);
        type.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        Field f = new Field("text", "this is some text that will have a length norm greater than 0", type);

        InputDocument doc = InputDocument.builder("id")
                .setDefaultAnalyzer(new StandardAnalyzer())
                .addField(f).build();

        try (Monitor monitor = new Monitor(new LuceneQueryParser("text"), new MatchAllPresearcher())) {
            monitor.update(new MonitorQuery("q", "length"));

            Matches<ExplainingMatch> matches = monitor.match(doc, ExplainingMatcher.FACTORY);
            DocumentMatches<ExplainingMatch> m = matches.getMatches("id");
            for (ExplainingMatch e : m) {
                Explanation expl = e.getExplanation();
                assertThat(expl.toString()).contains("norms omitted");
            }
        }
    }

}
