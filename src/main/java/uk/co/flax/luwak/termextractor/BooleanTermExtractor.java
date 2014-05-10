package uk.co.flax.luwak.termextractor;

import com.google.common.collect.Iterables;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import uk.co.flax.luwak.termextractor.weights.CompoundRuleWeightor;
import uk.co.flax.luwak.termextractor.weights.TermWeightor;

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

/**
 * Extract terms from a BooleanQuery, recursing into the BooleanClauses
 *
 * If the query is a pure conjunction, then this extractor will select the best
 * matching term from all the clauses and only extract that.
 */
public class BooleanTermExtractor extends Extractor<BooleanQuery> {

    private final TermWeightor weightor;

    public BooleanTermExtractor(TermWeightor weightor) {
        super(BooleanQuery.class);
        this.weightor = weightor;
    }

    public BooleanTermExtractor() {
        this(CompoundRuleWeightor.DEFAULT_WEIGHTOR);
    }

    @Override
    public void extract(BooleanQuery query, List<QueryTerm> terms, List<Extractor<?>> extractors) {

        Analyzer checker = new Analyzer(query);

        if (checker.isDisjunctionQuery()) {
            for (Query subquery : checker.getDisjunctions()) {
                extractTerms(subquery, terms, extractors);
            }
        }
        else if (checker.isConjunctionQuery()) {
            List<QueryTermList> termlists = new ArrayList<>();
            for (Query subquery : checker.getConjunctions()) {
                List<QueryTerm> subTerms = new ArrayList<>();
                extractTerms(subquery, subTerms, extractors);
                termlists.add(new QueryTermList(this.weightor, subTerms));
            }
            Iterables.addAll(terms, QueryTermList.selectBest(termlists));
        }
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
