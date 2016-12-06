package uk.co.flax.luwak.termextractor.treebuilder;
/*
 *   Copyright (c) 2016 Lemur Consulting Ltd.
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

import java.lang.reflect.Field;

import org.apache.lucene.queries.payloads.PayloadScoreQuery;
import org.apache.lucene.search.Query;
import uk.co.flax.luwak.termextractor.QueryAnalyzer;
import uk.co.flax.luwak.termextractor.QueryTreeBuilder;
import uk.co.flax.luwak.termextractor.querytree.QueryTree;

public class PayloadScoreQueryTreeBuilder extends QueryTreeBuilder<PayloadScoreQuery> {

    public static final PayloadScoreQueryTreeBuilder INSTANCE = new PayloadScoreQueryTreeBuilder();

    private final Field queryField;

    private PayloadScoreQueryTreeBuilder() {
        super(PayloadScoreQuery.class);
        try {
            queryField = PayloadScoreQuery.class.getDeclaredField("wrappedQuery");
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
        queryField.setAccessible(true);
    }

    @Override
    public QueryTree buildTree(QueryAnalyzer builder, PayloadScoreQuery query) {
        try {
            return builder.buildTree((Query) queryField.get(query));
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
