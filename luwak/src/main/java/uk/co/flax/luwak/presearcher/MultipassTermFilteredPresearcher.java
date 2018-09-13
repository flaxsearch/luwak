package uk.co.flax.luwak.presearcher;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;

import uk.co.flax.luwak.analysis.TermsEnumTokenStream;
import uk.co.flax.luwak.termextractor.querytree.QueryTree;
import uk.co.flax.luwak.termextractor.querytree.QueryTreeViewer;
import uk.co.flax.luwak.termextractor.querytree.TreeAdvancer;
import uk.co.flax.luwak.termextractor.querytree.TreeWeightor;

/*
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
 * Routes are selected using a {@link uk.co.flax.luwak.termextractor.querytree.TreeAdvancer}.
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

    protected final TreeAdvancer advancer;

    /**
     * Construct a new MultipassTermFilteredPresearcher
     * @param passes the number of times a query should be indexed
     * @param advancer the Advancer to use
     * @param weightor the TreeWeightor to use
     * @param components the PresearcherComponents to use
     */
    public MultipassTermFilteredPresearcher(int passes, TreeAdvancer advancer, TreeWeightor weightor, PresearcherComponent... components) {
        super(weightor, components);
        this.advancer = advancer;
        this.passes = passes;
    }

    /**
     * Construct a new MultipassTermFilteredPresearcher, using a MinWeightAdvancer
     * @param passes the number of times a query should be indexed
     * @param minWeight the minimum weight a query term can have to be advanced over
     * @param weightor the TreeWeightor to use
     * @param components the PresearcherComponents to use
     */
    public MultipassTermFilteredPresearcher(int passes, float minWeight, TreeWeightor weightor, PresearcherComponent... components) {
        this(passes, new TreeAdvancer.MinWeightTreeAdvancer(weightor, minWeight), weightor, components);
    }

    /**
     * Construct a new MultipassTermFilteredPresearcher, using the default TreeWeightor
     * @param passes the number of times a query should be indexed
     * @param advancer the {@link uk.co.flax.luwak.termextractor.querytree.TreeAdvancer} to use on subsequent passes
     * @param components the PresearcherComponents to use
     */
    public MultipassTermFilteredPresearcher(int passes, TreeAdvancer advancer, PresearcherComponent... components) {
        this(passes, advancer, TreeWeightor.DEFAULT_WEIGHTOR, components);
    }

    /**
     * Construct a new MultipassTermFilteredPresearcher, using the default TreeWeightor and a MinWeightAdvancer
     * @param passes the number of times a query should be indexed
     * @param minWeight the minimum weight a query term can have to be advanced over
     * @param components the PresearcherComponents to use
     */
    public MultipassTermFilteredPresearcher(int passes, float minWeight, PresearcherComponent... components) {
        this(passes, minWeight, TreeWeightor.DEFAULT_WEIGHTOR, components);
    }

    @Override
    protected DocumentQueryBuilder getQueryBuilder() {
        return new MultipassDocumentQueryBuilder();
    }

    static String field(String field, int pass) {
        return field + "_" + pass;
    }

    private class MultipassDocumentQueryBuilder implements DocumentQueryBuilder {

        BooleanQuery.Builder[] queries = new BooleanQuery.Builder[passes];
        List<List<Term>> terms = new ArrayList<List<Term>>(passes);

        public MultipassDocumentQueryBuilder() {
            for (int i = 0; i < queries.length; i++) {
                queries[i] = new BooleanQuery.Builder();
                terms.add(i, new ArrayList<Term>());
            }
        }

        @Override
        public void addTerm(String field, BytesRef term) throws IOException {
            for (int i = 0; i < passes; i++) {
                terms.get(i).add(new Term(field(field, i), term));
            }
        }

        @Override
        public Query build() {
            BooleanQuery.Builder parent = new BooleanQuery.Builder();
            for (int i = 0; i < passes; i++) {
                if (terms.get(i).size() == 1) {
                    parent.add(new TermQuery(terms.get(i).iterator().next()), BooleanClause.Occur.MUST);
                } else {
                    BooleanQuery.Builder bq = new BooleanQuery.Builder();
                    for (Term term : terms.get(i)) {
                        bq.add(new TermQuery(term), BooleanClause.Occur.SHOULD);
                    }
                    parent.add(bq.build(), BooleanClause.Occur.MUST);
                }
            }
            return parent.build();
        }
    }

    @Override
    public Document buildQueryDocument(QueryTree querytree) {

        Document doc = new Document();

        for (int i = 0; i < passes; i++) {
            Map<String, BytesRefHash> fieldTerms = collectTerms(querytree);
            debug(querytree, fieldTerms);
            for (Map.Entry<String, BytesRefHash> entry : fieldTerms.entrySet()) {
                // we add the index terms once under a suffixed field for the multipass query, and
                // once under the plan field name for the TermsEnumTokenFilter
                doc.add(new Field(field(entry.getKey(), i),
                        new TermsEnumTokenStream(new BytesRefHashIterator(entry.getValue())), QUERYFIELDTYPE));
                doc.add(new Field(entry.getKey(),
                        new TermsEnumTokenStream(new BytesRefHashIterator(entry.getValue())), QUERYFIELDTYPE));
            }
            extractor.advancePhase(querytree, advancer);
        }

        return doc;
    }

    /**
     * Override to debug queryindexing
     * @param tree the current QueryTree
     * @param terms the terms collected from it
     */
    protected void debug(QueryTree tree, Map<String, BytesRefHash> terms) {}

    @Override
    public void showQueryTree(Query query, PrintStream out) {
        QueryTreeViewer.view(extractor.buildTree(query), extractor.weightor, advancer, out);
    }
}
