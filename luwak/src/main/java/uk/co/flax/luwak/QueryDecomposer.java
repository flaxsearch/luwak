package uk.co.flax.luwak;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;

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
 * Split a disjunction query into its consituent parts, so that they can be indexed
 * and run separately in the Monitor.
 */
public class QueryDecomposer {

    /**
     * Split a query up into individual parts that can be indexed and run separately
     * @param q the query
     * @return a collection of subqueries
     */
    public Collection<Query> decompose(Query q) {

        if (q instanceof BooleanQuery)
            return decomposeBoolean((BooleanQuery) q);

        return listOf(q);
    }

    private static Collection<Query> listOf(Query q) {
        List<Query> qs = new ArrayList<>(1);
        qs.add(q);
        return qs;
    }

    /**
     * Apply a boost to a query
     * @param q the query
     * @param boost the boost
     * @return the boosted query
     */
    public static Query boost(Query q, float boost) {
        q.setBoost(boost * q.getBoost());
        return q;
    }

    /**
     * Decompose a {@link org.apache.lucene.search.BooleanQuery}
     * @param q the boolean query
     * @return a collection of subqueries
     */
    public Collection<Query> decomposeBoolean(BooleanQuery q) {
        if (q.getMinimumNumberShouldMatch() > 1)
            return listOf(q);

        List<Query> subqueries = new LinkedList<>();
        List<Query> exclusions = new LinkedList<>();
        List<Query> mandatory = new LinkedList<>();

        float parentBoost = q.getBoost();

        for (BooleanClause clause : q) {
            if (clause.getOccur() == BooleanClause.Occur.MUST)
                mandatory.add(clause.getQuery());
            else if (clause.getOccur() == BooleanClause.Occur.MUST_NOT)
                exclusions.add(clause.getQuery());
            else {
                for (Query subQuery : decompose(clause.getQuery())) {
                    subqueries.add(boost(subQuery, parentBoost));
                }
            }
        }

        // More than one MUST clause, or a single MUST clause with disjunctions
        if (mandatory.size() > 1 || (mandatory.size() == 1 && subqueries.size() > 0))
            return listOf(q);

        // If we only have a single MUST clause and no SHOULD clauses, then we can
        // decompose the MUST clause instead
        if (mandatory.size() == 1) {
            for (Query subQuery : decompose(mandatory.get(0))) {
                subqueries.add(boost(subQuery, parentBoost));
            }
        }

        if (exclusions.size() == 0)
            return subqueries;

        // If there are exclusions, then we need to add them to all the decomposed
        // queries
        List<Query> rewrittenSubqueries = new ArrayList<>(subqueries.size());
        for (Query subquery : subqueries) {
            BooleanQuery bq = new BooleanQuery();
            bq.add(subquery, BooleanClause.Occur.MUST);
            for (Query ex : exclusions) {
                bq.add(ex, BooleanClause.Occur.MUST_NOT);
            }
            rewrittenSubqueries.add(bq);
        }
        return rewrittenSubqueries;
    }
}
