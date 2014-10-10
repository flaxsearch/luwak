package uk.co.flax.luwak.presearcher;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.EmptyTokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import uk.co.flax.luwak.InputDocument;

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

/**
 * PresearcherComponent that allows you to filter queries out by a field on
 * each document.
 *
 * Queries are assigned field values by passing them as part of the metadata
 * on a MonitorQuery.
 */
public class FieldFilterPresearcherComponent extends PresearcherComponent {

    private final String field;

    /**
     * Create a new FieldFilterPresearcherComponent that filters queries on a
     * given field.
     *
     * @param field the field to filter on
     */
    public FieldFilterPresearcherComponent(String field) {
        this.field = field;
    }


    @Override
    public Query adjustPresearcherQuery(InputDocument doc, Query presearcherQuery) throws IOException {

        Query filterClause = buildFilterClause(doc);
        if (filterClause == null)
            return presearcherQuery;

        BooleanQuery bq = new BooleanQuery();
        bq.add(presearcherQuery, BooleanClause.Occur.MUST);
        bq.add(filterClause, BooleanClause.Occur.MUST);
        return bq;
    }

    private Query buildFilterClause(InputDocument doc) throws IOException {

        Terms terms = doc.asAtomicReader().fields().terms(field);
        if (terms == null)
            return null;

        BooleanQuery bq = new BooleanQuery();

        BytesRef term;
        TermsEnum te = terms.iterator(null);
        while ((term = te.next()) != null) {
            bq.add(new TermQuery(new Term(field, term.utf8ToString())), BooleanClause.Occur.SHOULD);
        }

        if (bq.clauses().size() == 0)
            return null;

        return bq;
    }

    @Override
    public void adjustQueryDocument(Document doc, Map<String, String> metadata) {
        if (metadata == null || !metadata.containsKey(field))
            return;
        doc.add(new StringField(field, metadata.get(field), Field.Store.YES));
    }

    @Override
    public TokenStream filterDocumentTokens(String field, TokenStream ts) {
        // We don't want tokens from this field to be present in the disjunction,
        // only in the extra filter query.  Otherwise, every doc that matches in
        // this field will be selected!
        if (this.field.equals(field))
            return new EmptyTokenStream();
        return ts;
    }
}
