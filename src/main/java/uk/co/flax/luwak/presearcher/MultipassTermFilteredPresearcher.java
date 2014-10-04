package uk.co.flax.luwak.presearcher;

import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import uk.co.flax.luwak.termextractor.querytree.QueryTree;

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

public class MultipassTermFilteredPresearcher extends TermFilteredPresearcher {

    private final int passes;

    public MultipassTermFilteredPresearcher(int passes, PresearcherComponent... components) {
        super(components);
        this.passes = passes;
    }

    @Override
    protected DocumentQueryBuilder getQueryBuilder() {
        return new MultipassDocumentQueryBuilder();
    }

    static String field(String field, int pass) {
        return field + "_" + pass;
    }

    private class MultipassDocumentQueryBuilder implements DocumentQueryBuilder {

        BooleanQuery[] queries = new BooleanQuery[passes];

        public MultipassDocumentQueryBuilder() {
            for (int i = 0; i < queries.length; i++) {
                queries[i] = new BooleanQuery();
            }
        }

        @Override
        public void addTerm(String field, String term) {
            for (int i = 0; i < passes; i++) {
                queries[i].add(new TermQuery(new Term(field(field, i), term)), BooleanClause.Occur.SHOULD);
            }
        }

        @Override
        public Query build() {
            BooleanQuery parent = new BooleanQuery();
            for (BooleanQuery child : queries) {
                parent.add(child, BooleanClause.Occur.MUST);
            }
            return parent;
        }
    }

    @Override
    public Document indexQuery(Query query) {

        QueryTree tree = extractor.buildTree(query);
        Document doc = new Document();

        for (int i = 0; i < passes; i++) {
            Map<String, StringBuilder> fieldTerms = collectTerms(tree);
            for (Map.Entry<String, StringBuilder> entry : fieldTerms.entrySet()) {
                // we add the index terms once under a suffixed field for the multipass query, and
                // once under the plan field name for the TermsEnumTokenFilter
                doc.add(new Field(field(entry.getKey(), i), entry.getValue().toString(), QUERYFIELDTYPE));
                doc.add(new Field(entry.getKey(), entry.getValue().toString(), QUERYFIELDTYPE));
            }
            extractor.advancePhase(tree);
        }

        return doc;
    }
}
