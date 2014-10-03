package uk.co.flax.luwak.termextractor.treebuilder;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.queries.BooleanFilter;
import org.apache.lucene.queries.FilterClause;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import uk.co.flax.luwak.termextractor.QueryTreeBuilder;
import uk.co.flax.luwak.termextractor.QueryAnalyzer;
import uk.co.flax.luwak.termextractor.QueryTerm;
import uk.co.flax.luwak.termextractor.querytree.*;

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

/**
 * Extract terms from a BooleanQuery, recursing into the BooleanClauses
 *
 * If the query is a pure conjunction, then this extractor will select the best
 * matching term from all the clauses and only extract that.
 */
public abstract class BooleanQueryTreeBuilder<T> extends QueryTreeBuilder<T> {

    public BooleanQueryTreeBuilder(Class<T> cls) {
        super(cls);
    }

    protected abstract Clauses analyze(T query);

    @Override
    public QueryTree buildTree(QueryAnalyzer builder, T query) {

        Clauses clauses = analyze(query);

        if (clauses.isPureNegativeQuery())
            return new TermNode(builder.weightor, new QueryTerm("", "PURE NEGATIVE BOOLEAN", QueryTerm.Type.ANY));

        if (clauses.isDisjunctionQuery()) {
            return DisjunctionNode.build(builder.weightor, buildChildTrees(builder, clauses.getDisjunctions()));
        }

        return ConjunctionNode.build(builder.weightor, buildChildTrees(builder, clauses.getConjunctions()));
    }

    private List<QueryTree> buildChildTrees(QueryAnalyzer builder, List<Object> children) {
        List<QueryTree> trees = new ArrayList<>();
        for (Object child : children) {
            trees.add(builder.buildTree(child));
        }
        return trees;
    }

    public static class Clauses {

        final List<Object> disjunctions = new ArrayList<>();
        final List<Object> conjunctions = new ArrayList<>();
        final List<Object> negatives = new ArrayList<>();

        public boolean isConjunctionQuery() {
            return conjunctions.size() > 0;
        }

        public boolean isDisjunctionQuery() {
            return !isConjunctionQuery() && disjunctions.size() > 0;
        }

        public boolean isPureNegativeQuery() {
            return conjunctions.size() == 0 && disjunctions.size() == 0 && negatives.size() > 0;
        }

        public List<Object> getDisjunctions() {
            return disjunctions;
        }

        public List<Object> getConjunctions() {
            return conjunctions;
        }
    }

    public static class QueryBuilder extends BooleanQueryTreeBuilder<BooleanQuery> {

        public QueryBuilder() {
            super(BooleanQuery.class);
        }

        @Override
        protected Clauses analyze(BooleanQuery query) {
            Clauses clauses = new Clauses();
            for (BooleanClause clause : query.getClauses()) {
                if (clause.getQuery() instanceof MatchAllDocsQuery) {
                    continue;       // ignored for term extraction
                }
                if (clause.getOccur() == BooleanClause.Occur.MUST) {
                    clauses.conjunctions.add(clause.getQuery());
                }
                if (clause.getOccur() == BooleanClause.Occur.SHOULD) {
                    clauses.disjunctions.add(clause.getQuery());
                }
                if (clause.getOccur() == BooleanClause.Occur.MUST_NOT) {
                    clauses.negatives.add(clause.getQuery());
                }
            }
            return clauses;
        }
    }

    public static class FilterBuilder extends BooleanQueryTreeBuilder<BooleanFilter> {

        public FilterBuilder() {
            super(BooleanFilter.class);
        }

        @Override
        protected Clauses analyze(BooleanFilter filter) {
            Clauses clauses = new Clauses();
            for (FilterClause clause : filter.clauses()) {
                if (clause.getOccur() == BooleanClause.Occur.MUST) {
                    clauses.conjunctions.add(clause.getFilter());
                }
                if (clause.getOccur() == BooleanClause.Occur.SHOULD) {
                    clauses.disjunctions.add(clause.getFilter());
                }
            }
            return clauses;
        }
    }

}
