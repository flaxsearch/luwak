package uk.co.flax.luwak.termextractor.treebuilder;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanQuery;
import uk.co.flax.luwak.termextractor.QueryAnalyzer;
import uk.co.flax.luwak.termextractor.QueryTreeBuilder;
import uk.co.flax.luwak.termextractor.querytree.ConjunctionNode;
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
 * Extract terms from an IntervalFilterQuery
 */
public class SpanNearQueryTreeBuilder extends QueryTreeBuilder<SpanNearQuery> {

    public SpanNearQueryTreeBuilder() {
        super(SpanNearQuery.class);
    }

    @Override
    public QueryTree buildTree(QueryAnalyzer builder, SpanNearQuery query) {
        List<QueryTree> children = new ArrayList<>();
        for (SpanQuery subq : query.getClauses()) {
            children.add(builder.buildTree(subq));
        }
        return ConjunctionNode.build(children);
    }
}
