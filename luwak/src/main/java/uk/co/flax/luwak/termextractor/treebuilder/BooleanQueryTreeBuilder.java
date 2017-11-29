package uk.co.flax.luwak.termextractor.treebuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import uk.co.flax.luwak.termextractor.QueryAnalyzer;
import uk.co.flax.luwak.termextractor.QueryTreeBuilder;
import uk.co.flax.luwak.termextractor.querytree.AnyNode;
import uk.co.flax.luwak.termextractor.querytree.ConjunctionNode;
import uk.co.flax.luwak.termextractor.querytree.DisjunctionNode;
import uk.co.flax.luwak.termextractor.querytree.QueryTree;

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

/**
 * Extract terms from a BooleanQuery, recursing into the BooleanClauses
 *
 * If the query is a pure conjunction, then this extractor will select the best
 * matching term from all the clauses and only extract that.
 */
public class BooleanQueryTreeBuilder extends QueryTreeBuilder<BooleanQuery> {

    public BooleanQueryTreeBuilder() {
        super(BooleanQuery.class);
    }

    protected Clauses analyze(BooleanQuery query) {
        Clauses clauses = new Clauses();
        for (BooleanClause clause : query) {
            if (clause.getOccur() == BooleanClause.Occur.MUST || clause.getOccur() == BooleanClause.Occur.FILTER) {
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

    @Override
    public QueryTree buildTree(QueryAnalyzer builder, BooleanQuery query) {

        Clauses clauses = analyze(query);

        if (clauses.isPureNegativeQuery())
            return new AnyNode("PURE NEGATIVE BOOLEAN");

        if (clauses.isDisjunctionQuery()) {
            return DisjunctionNode.build(buildChildTrees(builder, clauses.getDisjunctions()));
        }

        return ConjunctionNode.build(buildChildTrees(builder, clauses.getConjunctions()));
    }

    private List<QueryTree> buildChildTrees(QueryAnalyzer builder, List<Query> children) {
        return children.stream().map(builder::buildTree).collect(Collectors.toList());
    }

    public static class Clauses {

        final List<Query> disjunctions = new ArrayList<>();
        final List<Query> conjunctions = new ArrayList<>();
        final List<Query> negatives = new ArrayList<>();

        public boolean isConjunctionQuery() {
            return conjunctions.size() > 0;
        }

        public boolean isDisjunctionQuery() {
            return !isConjunctionQuery() && disjunctions.size() > 0;
        }

        public boolean isPureNegativeQuery() {
            return conjunctions.size() == 0 && disjunctions.size() == 0 && negatives.size() > 0;
        }

        public List<Query> getDisjunctions() {
            return disjunctions;
        }

        public List<Query> getConjunctions() {
            return conjunctions;
        }
    }

}
