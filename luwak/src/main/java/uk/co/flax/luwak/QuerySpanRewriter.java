package uk.co.flax.luwak;

/*
 *   Copyright (c) 2015 Lemur Consulting Ltd.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.search.*;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;

public class QuerySpanRewriter {

    public List<SpanQuery> rewrite(Query query) {
        List<SpanQuery> spanqueries = new ArrayList<>();
        rewrite(query, spanqueries);
        return spanqueries;
    }

    protected void rewrite(Query query, List<SpanQuery> spanQueries) {
        if (query instanceof SpanQuery) {
            spanQueries.add((SpanQuery)query);
        }
        else if (query instanceof TermQuery) {
            spanQueries.add(new SpanTermQuery(((TermQuery)query).getTerm()));
        }
        else if (query instanceof MultiTermQuery) {
            spanQueries.add(new SpanMultiTermQueryWrapper<>((MultiTermQuery)query));
        }
        else if (query instanceof BooleanQuery) {
            for (BooleanClause clause : (BooleanQuery) query) {
                if (clause.isScoring())
                    rewrite(clause.getQuery(), spanQueries);
            }
        }
        else if (query instanceof DisjunctionMaxQuery) {
            for (Query subquery : (DisjunctionMaxQuery) query) {
                rewrite(subquery, spanQueries);
            }
        }
    }

}
