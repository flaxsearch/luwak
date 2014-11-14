package uk.co.flax.luwak;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;

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

public class QueryDecomposer {

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

    public Collection<Query> decomposeBoolean(BooleanQuery q) {
        if (q.getMinimumNumberShouldMatch() > 1)
            return listOf(q);

        List<Query> subqueries = new LinkedList<>();
        List<Query> exclusions = new LinkedList<>();
        List<Query> mandatory = new LinkedList<>();

        for (BooleanClause clause : q) {
            if (clause.getOccur() == BooleanClause.Occur.MUST)
                mandatory.add(clause.getQuery());
            else if (clause.getOccur() == BooleanClause.Occur.MUST_NOT)
                exclusions.add(clause.getQuery());
            else
                subqueries.addAll(decompose(clause.getQuery()));
        }

        // More than one MUST clause, or a single MUST clause with disjunctions
        if (mandatory.size() > 1 || (mandatory.size() == 1 && subqueries.size() > 0))
            return listOf(q);

        // If we only have a single MUST clause and no SHOULD clauses, then we can
        // decompose the MUST clause instead
        if (mandatory.size() == 1)
            subqueries.addAll(decompose(mandatory.get(0)));

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
