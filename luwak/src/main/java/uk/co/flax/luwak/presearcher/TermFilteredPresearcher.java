package uk.co.flax.luwak.presearcher;

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

import java.io.IOException;
import java.util.*;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import uk.co.flax.luwak.InputDocument;
import uk.co.flax.luwak.Presearcher;
import uk.co.flax.luwak.analysis.TermsEnumTokenStream;
import uk.co.flax.luwak.termextractor.QueryAnalyzer;
import uk.co.flax.luwak.termextractor.QueryTerm;
import uk.co.flax.luwak.termextractor.querytree.QueryTree;
import uk.co.flax.luwak.termextractor.querytree.TreeWeightor;

/**
 * Presearcher implementation that uses terms extracted from queries to index
 * them in the Monitor, and builds a BooleanQuery from InputDocuments to match
 * them.
 *
 * This Presearcher uses a QueryTermExtractor to extract terms from queries.
 */
public class TermFilteredPresearcher extends Presearcher {

    static {
        BooleanQuery.setMaxClauseCount(Integer.MAX_VALUE);
    }

    protected final QueryAnalyzer extractor;

    private final List<PresearcherComponent> components = new ArrayList<>();

    public static final String ANYTOKEN_FIELD = "__anytokenfield";

    public static final String ANYTOKEN = "__ANYTOKEN__";

    public TermFilteredPresearcher(TreeWeightor weightor, PresearcherComponent... components) {
        this.extractor = QueryAnalyzer.fromComponents(weightor, components);
        this.components.addAll(Arrays.asList(components));
    }

    public TermFilteredPresearcher(PresearcherComponent... components) {
        this(TreeWeightor.DEFAULT_WEIGHTOR, components);
    }

    public TermFilteredPresearcher() {
        this(TreeWeightor.DEFAULT_WEIGHTOR);
    }

    @Override
    public final Query buildQuery(InputDocument doc, PerFieldTokenFilter filter) {
        try {
            LeafReader reader = doc.asAtomicReader();
            DocumentQueryBuilder queryBuilder = getQueryBuilder();

            for (String field : reader.fields()) {

                TokenStream ts = new TermsEnumTokenStream(reader.terms(field).iterator(null));
                for (PresearcherComponent component : components) {
                    ts = component.filterDocumentTokens(field, ts);
                }
                ts = filter.filter(field, ts);

                CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
                while (ts.incrementToken()) {
                    queryBuilder.addTerm(field, termAtt.toString());
                }

            }
            Query presearcherQuery = queryBuilder.build();

            BooleanQuery bq = new BooleanQuery();
            bq.add(presearcherQuery, BooleanClause.Occur.SHOULD);
            bq.add(new TermQuery(new Term(ANYTOKEN_FIELD, ANYTOKEN)), BooleanClause.Occur.SHOULD);
            presearcherQuery = bq;

            for (PresearcherComponent component : components) {
                presearcherQuery = component.adjustPresearcherQuery(doc, presearcherQuery);
            }

            return presearcherQuery;
        }
        catch (IOException e) {
            // We're a MemoryIndex, so this shouldn't happen...
            throw new RuntimeException(e);
        }
    }

    protected DocumentQueryBuilder getQueryBuilder() {
        return new DocumentQueryBuilder() {

            BooleanQuery bq = new BooleanQuery();

            @Override
            public void addTerm(String field, String term) {
                bq.add(new TermQuery(new Term(field, term)), BooleanClause.Occur.SHOULD);
            }

            @Override
            public Query build() {
                return bq;
            }
        };
    }

    public static final FieldType QUERYFIELDTYPE;
    static {
        QUERYFIELDTYPE = new FieldType(TextField.TYPE_STORED);
        QUERYFIELDTYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        QUERYFIELDTYPE.freeze();
    }

    @Override
    public final Document indexQuery(Query query, Map<String, String> metadata) {

        QueryTree querytree = extractor.buildTree(query);
        Document doc = buildQueryDocument(querytree);

        for (PresearcherComponent component : components) {
            component.adjustQueryDocument(doc, metadata);
        }

        return doc;
    }

    protected Document buildQueryDocument(QueryTree querytree) {
        Map<String, StringBuilder> fieldTerms = collectTerms(querytree);
        Document doc = new Document();
        for (Map.Entry<String, StringBuilder> entry : fieldTerms.entrySet()) {
            doc.add(new Field(entry.getKey(), entry.getValue().toString(), QUERYFIELDTYPE));
        }
        return doc;
    }

    protected Map<String, StringBuilder> collectTerms(QueryTree tree) {

        Map<String, StringBuilder> fieldTerms = new HashMap<>();

        for (QueryTerm queryTerm : extractor.collectTerms(tree)) {
            if (queryTerm.type.equals(QueryTerm.Type.ANY)) {
                if (!fieldTerms.containsKey(ANYTOKEN_FIELD))
                    fieldTerms.put(ANYTOKEN_FIELD, new StringBuilder(ANYTOKEN));
            }
            else {
                if (!fieldTerms.containsKey(queryTerm.field))
                    fieldTerms.put(queryTerm.field, new StringBuilder());

                //noinspection MismatchedQueryAndUpdateOfStringBuilder
                StringBuilder termslist = fieldTerms.get(queryTerm.field);
                if (queryTerm.type.equals(QueryTerm.Type.EXACT)) {
                    termslist.append(" ").append(queryTerm.term);
                } else {
                    termslist.append(" ").append(queryTerm.term);
                    for (PresearcherComponent component : components) {
                        String extratoken = component.extraToken(queryTerm);
                        if (extratoken != null)
                            termslist.append(" ").append(extratoken);
                    }
                }
            }
        }

        return fieldTerms;
    }


}
