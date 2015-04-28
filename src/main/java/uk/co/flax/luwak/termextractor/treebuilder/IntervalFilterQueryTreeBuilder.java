package uk.co.flax.luwak.termextractor.treebuilder;

import java.lang.reflect.Field;

import org.apache.lucene.search.intervals.IntervalFilterQuery;
import uk.co.flax.luwak.termextractor.QueryTreeBuilder;
import uk.co.flax.luwak.termextractor.QueryAnalyzer;
import uk.co.flax.luwak.termextractor.querytree.QueryTree;

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
 * Extract terms from an IntervalFilterQuery
 */
public class IntervalFilterQueryTreeBuilder extends QueryTreeBuilder<IntervalFilterQuery> {

    public IntervalFilterQueryTreeBuilder() {
        super(IntervalFilterQuery.class);
    }

    @Override
    public QueryTree buildTree(QueryAnalyzer builder, IntervalFilterQuery query) {
        try {
            Field field = IntervalFilterQuery.class.getDeclaredField("inner");
            field.setAccessible(true);
            return builder.buildTree(field.get(query));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
