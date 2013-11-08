package uk.co.flax.luwak.termextractor;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;

import java.util.ArrayList;
import java.util.List;

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

public class BooleanTermExtractor extends Extractor<BooleanQuery> {

    public BooleanTermExtractor() {
        super(BooleanQuery.class);
    }

    @Override
    public void extract(BooleanQuery query, List<QueryTerm> terms,
                        QueryTermExtractor queryTermExtractor) {

        Analyzer checker = new Analyzer(query);

        if (checker.isDisjunctionQuery()) {
            for (Query subquery : checker.getDisjunctions()) {
                queryTermExtractor.extractTerms(subquery, terms);
            }
        }
        else if (checker.isConjunctionQuery()) {
            List<QueryTerm> bestTerms = null;
            for (Query subquery : checker.getConjunctions()) {
                List<QueryTerm> subTerms = new ArrayList<>();
                queryTermExtractor.extractTerms(subquery, subTerms);
                bestTerms = selectBestTerms(bestTerms, subTerms);
            }
            terms.addAll(bestTerms);
        }
    }

    private int compareTypeCounts(List<QueryTerm> first, List<QueryTerm> second, QueryTerm.Type type) {
        int firstCount = countType(first, type);
        int secondCount = countType(second, type);
        if (firstCount == 0 && secondCount > 0)
            return -1;
        if (secondCount == 0 && firstCount > 0)
            return 1;
        return 0;
    }

    protected List<QueryTerm> selectBestTerms(List<QueryTerm> first, List<QueryTerm> second) {
        if (first == null)
            return second;

        // If either termlist contains no ANY terms, and the other one does contain some
        // return the list with none.
        switch (compareTypeCounts(first, second, QueryTerm.Type.ANY)) {
            case -1: return first;
            case 1: return second;
        }

        // If either termlist contains no wildcard terms, and the other does contains some
        // return the list with none
        switch (compareTypeCounts(first, second, QueryTerm.Type.WILDCARD)) {
            case -1: return first;
            case 1: return second;
        }

        if (second.size() < first.size())
            return second;

        return first;
    }

    private int countType(List<QueryTerm> terms, QueryTerm.Type type) {
        int c = 0;
        for (QueryTerm term : terms) {
            if (term.type == type) c++;
        }
        return c;
    }

    public static class Analyzer {

        List<Query> disjunctions = new ArrayList<>();
        List<Query> conjunctions = new ArrayList<>();

        public Analyzer(BooleanQuery query) {
            for (BooleanClause clause : query.getClauses()) {
                if (clause.getOccur() == BooleanClause.Occur.MUST) {
                    conjunctions.add(clause.getQuery());
                }
                if (clause.getOccur() == BooleanClause.Occur.SHOULD) {
                    disjunctions.add(clause.getQuery());
                }
            }
        }

        public boolean isConjunctionQuery() {
            return conjunctions.size() > 0;
        }

        public boolean isDisjunctionQuery() {
            return !isConjunctionQuery() && disjunctions.size() > 0;
        }

        public List<Query> getDisjunctions() {
            return disjunctions;
        }

        public List<Query> getConjunctions() {
            return conjunctions;
        }
    }
}
