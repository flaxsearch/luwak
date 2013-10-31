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
            List<QueryTerm> subTerms = new ArrayList<>();
            for (Query subquery : checker.getConjunctions()) {
                queryTermExtractor.extractTerms(subquery, subTerms);
            }
            terms.add(bestTerm(subTerms));
        }
    }

    protected QueryTerm bestTerm(List<QueryTerm> terms) {
        for (QueryTerm term : terms) {
            if (term.type == QueryTerm.Type.EXACT)
                return term;
        }
        for (QueryTerm term : terms) {
            if (term.type == QueryTerm.Type.WILDCARD)
                return term;
        }
        return terms.get(0);
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
