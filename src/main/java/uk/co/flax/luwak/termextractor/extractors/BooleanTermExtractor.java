package uk.co.flax.luwak.termextractor.extractors;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Iterables;
import org.apache.lucene.queries.BooleanFilter;
import org.apache.lucene.queries.FilterClause;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import uk.co.flax.luwak.termextractor.Extractor;
import uk.co.flax.luwak.termextractor.QueryTerm;
import uk.co.flax.luwak.termextractor.weights.TermWeightor;

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
public abstract class BooleanTermExtractor<T> extends Extractor<T> {

    private final TermWeightor weightor;

    public BooleanTermExtractor(Class<T> cls, TermWeightor weightor) {
        super(cls);
        this.weightor = weightor;
    }

    protected abstract Clauses analyze(T query);

    @Override
    public void extract(T query, List<QueryTerm> terms, List<Extractor<?>> extractors) {

        Clauses clauses = analyze(query);

        if (clauses.isPureNegativeQuery()) {
            terms.add(new QueryTerm("", "PURE NEGATIVE BOOLEAN", QueryTerm.Type.ANY));
        }
        else if (clauses.isDisjunctionQuery()) {
            for (Object subquery : clauses.getDisjunctions()) {
                extractTerms(subquery, terms, extractors);
            }
        }
        else if (clauses.isConjunctionQuery()) {
            List<List<QueryTerm>> termlists = new ArrayList<>();
            for (Object subquery : clauses.getConjunctions()) {
                List<QueryTerm> subTerms = new ArrayList<>();
                extractTerms(subquery, subTerms, extractors);
                termlists.add(subTerms);
            }
            Iterables.addAll(terms, this.weightor.selectBest(termlists));
        }
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

    public static class QueryExtractor extends BooleanTermExtractor<BooleanQuery> {

        public QueryExtractor(TermWeightor weightor) {
            super(BooleanQuery.class, weightor);
        }

        @Override
        protected Clauses analyze(BooleanQuery query) {
            Clauses clauses = new Clauses();
            for (BooleanClause clause : query.getClauses()) {
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

    public static class FilterExtractor extends BooleanTermExtractor<BooleanFilter> {

        public FilterExtractor(TermWeightor weightor) {
            super(BooleanFilter.class, weightor);
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
