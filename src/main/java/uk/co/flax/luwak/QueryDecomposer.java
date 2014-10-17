package uk.co.flax.luwak;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import com.google.common.collect.ImmutableList;
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

        return ImmutableList.of(q);
    }

    public Collection<Query> decomposeBoolean(BooleanQuery q) {
        if (q.getMinimumNumberShouldMatch() > 1)
            return ImmutableList.<Query>of(q);

        List<Query> subqueries = new LinkedList<>();
        List<Query> exclusions = new LinkedList<>();

        for (BooleanClause clause : q) {
            // Any MUST clauses mean we can't decompose
            if (clause.getOccur() == BooleanClause.Occur.MUST)
                return ImmutableList.<Query>of(q);
            if (clause.getOccur() == BooleanClause.Occur.MUST_NOT)
                exclusions.add(clause.getQuery());
            else
                subqueries.addAll(decompose(clause.getQuery()));
        }

        if (exclusions.size() == 0)
            return subqueries;

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
