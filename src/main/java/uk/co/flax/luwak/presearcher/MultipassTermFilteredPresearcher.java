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
import uk.co.flax.luwak.termextractor.querytree.TreeWeightor;

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
 * A TermFilteredPresearcher that indexes queries multiple times, with terms collected
 * from different routes through a querytree.  Each route will produce a set of terms
 * that are *sufficient* to select the query, and are indexed into a separate, suffixed field.
 *
 * Incoming InputDocuments are then converted to a set of Disjunction queries over each
 * suffixed field, and these queries are combined into a conjunction query, such that the
 * document's set of terms must match a term from each route.
 *
 * This allows filtering out of documents that contain one half of a two-term phrase query, for
 * example.  The query {@code "hello world"} will be indexed twice, once under 'hello' and once
 * under 'world'.  A document containing the terms "hello there" would match the first field,
 * but not the second, and so would not be selected for matching.
 *
 * The number of passes the presearcher makes is configurable.  More passes will improve the
 * selected/matched ratio, but will take longer to index and will use more RAM.
 */
public class MultipassTermFilteredPresearcher extends TermFilteredPresearcher {

    private final int passes;

    /**
     * Construct a new MultipassTermFilteredPresearcher
     * @param passes the number of times a query should be indexed
     * @param weightor the TreeWeightor to use
     * @param components the PresearcherComponents to use
     */
    public MultipassTermFilteredPresearcher(int passes, TreeWeightor weightor, PresearcherComponent... components) {
        super(weightor, components);
        this.passes = passes;
    }

    /**
     * Construct a new MultipassTermFilteredPresearcher, using the default TreeWeightor
     * @param passes the number of times a query should be indexed
     * @param components the PresearcherComponents to use
     */
    public MultipassTermFilteredPresearcher(int passes, PresearcherComponent... components) {
        this(passes, TreeWeightor.DEFAULT_WEIGHTOR, components);
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
            debug(tree, fieldTerms);
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

    /**
     * Override to debug queryindexing
     * @param tree the current QueryTree
     * @param terms the terms collected from it
     */
    protected void debug(QueryTree tree, Map<String, StringBuilder> terms) {}
}
