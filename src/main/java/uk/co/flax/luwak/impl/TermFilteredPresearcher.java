package uk.co.flax.luwak.impl;/*
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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import uk.co.flax.luwak.InputDocument;
import uk.co.flax.luwak.Monitor;
import uk.co.flax.luwak.Presearcher;
import uk.co.flax.luwak.termextractor.QueryTerm;
import uk.co.flax.luwak.termextractor.QueryTermExtractor;
import uk.co.flax.luwak.util.TermsEnumTokenStream;
import uk.co.flax.luwak.util.TokenStreamBooleanQuery;

import java.io.IOException;

public class TermFilteredPresearcher implements Presearcher {

    private final QueryTermExtractor extractor;
    private final Monitor monitor;

    private final String ANYTOKEN = "__ANYTOKEN__";

    public TermFilteredPresearcher(Monitor monitor) {
        this(monitor, new QueryTermExtractor());
    }

    public TermFilteredPresearcher(Monitor monitor, QueryTermExtractor extractor) {
        this.monitor = monitor;
        this.extractor = extractor;
    }

    @Override
    public final Query buildQuery(InputDocument doc) {
        try {
            AtomicReader reader = doc.asAtomicReader();
            BooleanQuery bq = new BooleanQuery();
            for (String field : reader.fields()) {
                TermsEnum te = reader.terms(field).iterator(null);
                TokenStream ts = filterInputDocumentTokens(field, new TermsEnumTokenStream(te));
                bq.add(TokenStreamBooleanQuery.fromTokenStream(field, ts),
                        BooleanClause.Occur.SHOULD);
                bq.add(new TermQuery(new Term(field, ANYTOKEN)), BooleanClause.Occur.SHOULD);
            }
            return bq;
        }
        catch (IOException e) {
            // We're a MemoryIndex, so this shouldn't happen...
            throw new RuntimeException(e);
        }
    }

    protected TokenStream filterInputDocumentTokens(String field, TokenStream ts) {
        return monitor.filterTokenStream(field, ts);
    }

    @Override
    public final void indexQuery(Document doc, Query query) {
        for (QueryTerm queryTerm : extractor.extract(query)) {
            if (queryTerm.type == QueryTerm.Type.ANY)
                doc.add(new StringField(queryTerm.field, ANYTOKEN, Field.Store.NO));
            else
                doc.add(new StringField(queryTerm.field, queryTerm.term, Field.Store.NO));
        }
    }
}
