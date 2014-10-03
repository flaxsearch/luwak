package uk.co.flax.luwak.presearcher;/*
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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import uk.co.flax.luwak.InputDocument;
import uk.co.flax.luwak.Presearcher;
import uk.co.flax.luwak.analysis.TermsEnumTokenStream;
import uk.co.flax.luwak.termextractor.QueryTreeBuilder;
import uk.co.flax.luwak.termextractor.QueryTerm;
import uk.co.flax.luwak.termextractor.QueryAnalyzer;
import uk.co.flax.luwak.termextractor.querytree.QueryTree;
import uk.co.flax.luwak.termextractor.querytree.TreeWeightor;

/**
 * Presearcher implementation that uses terms extracted from queries to index
 * them in the Monitor, and builds a BooleanQuery from InputDocuments to match
 * them.
 *
 * This Presearcher uses a QueryTermExtractor to extract terms from queries.
 */
public class TermFilteredPresearcher implements Presearcher {

    static {
        BooleanQuery.setMaxClauseCount(Integer.MAX_VALUE);
    }

    protected final QueryAnalyzer extractor;

    public TermFilteredPresearcher(TreeWeightor weightor, QueryTreeBuilder... queryTreeBuilders) {
        this.extractor = new QueryAnalyzer(weightor, queryTreeBuilders);
    }

    public TermFilteredPresearcher(QueryTreeBuilder... queryTreeBuilders) {
        this(TreeWeightor.DEFAULT_WEIGHTOR, queryTreeBuilders);
    }

    @Override
    public final Query buildQuery(InputDocument doc, PerFieldTokenFilter filter) {
        try {
            AtomicReader reader = doc.asAtomicReader();
            BooleanQuery bq = new BooleanQuery();
            for (String field : reader.fields()) {

                TermsEnum te = reader.terms(field).iterator(null);
                TokenStream ts =
                        filter.filter(field, filterInputDocumentTokens(field, new TermsEnumTokenStream(te)));

                CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
                while (ts.incrementToken()) {
                    bq.add(new TermQuery(new Term(field, termAtt.toString())), BooleanClause.Occur.SHOULD);
                }

            }
            return bq;
        }
        catch (IOException e) {
            // We're a MemoryIndex, so this shouldn't happen...
            throw new RuntimeException(e);
        }
    }

    protected TokenStream filterInputDocumentTokens(String field, TokenStream ts) throws IOException {

        return new TokenFilter(ts) {

            boolean finished = false;
            CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

            @Override
            public final boolean incrementToken() throws IOException {
                if (input.incrementToken())
                    return true;
                if (finished)
                    return false;
                finished = true;
                termAtt.setEmpty().append(extractor.getAnyToken());
                return true;
            }
        };

    }

    public static final FieldType QUERYFIELDTYPE;
    static {
        QUERYFIELDTYPE = new FieldType(TextField.TYPE_STORED);
        QUERYFIELDTYPE.setIndexOptions(FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        QUERYFIELDTYPE.freeze();
    }

    @Override
    public final Document indexQuery(Query query) {

        Map<String, StringBuilder> fieldTerms = new HashMap<>();
        QueryTree querytree = extractor.buildTree(query);
        for (QueryTerm queryTerm : extractor.collectTerms(querytree)) {

            if (!fieldTerms.containsKey(queryTerm.field))
                fieldTerms.put(queryTerm.field, new StringBuilder());

            //noinspection MismatchedQueryAndUpdateOfStringBuilder
            StringBuilder termslist = fieldTerms.get(queryTerm.field);
            switch (queryTerm.type) {
                case WILDCARD:
                    termslist.append(" ").append(queryTerm.term);
                    // fall through
                case ANY:
                    termslist.append(" ").append(extractor.getAnyToken());
                    break;
                case EXACT:
                    termslist.append(" ").append(queryTerm.term);
            }
        }

        Document doc = new Document();
        for (Map.Entry<String, StringBuilder> entry : fieldTerms.entrySet()) {
            doc.add(new Field(entry.getKey(), entry.getValue().toString(), QUERYFIELDTYPE));
        }

        return doc;
    }

}
