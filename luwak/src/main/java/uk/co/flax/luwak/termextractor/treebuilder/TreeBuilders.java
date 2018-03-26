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
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.lucene.queries.function.BoostedQuery;
import org.apache.lucene.queries.function.FunctionScoreQuery;
import org.apache.lucene.search.*;
import org.apache.lucene.search.spans.*;
import uk.co.flax.luwak.termextractor.QueryAnalyzer;
import uk.co.flax.luwak.termextractor.QueryTerm;
import uk.co.flax.luwak.termextractor.QueryTreeBuilder;
import uk.co.flax.luwak.termextractor.querytree.*;
import uk.co.flax.luwak.util.CollectionUtils;

public class TreeBuilders {

    public static final QueryTreeBuilder<Query> ANY_NODE_BUILDER = new QueryTreeBuilder<Query>(Query.class) {
        @Override
        public QueryTree buildTree(QueryAnalyzer builder, Query query) {
            return new AnyNode("Cannot filter on query of type " + query.getClass().getName());
        }
    };

    private static final Function<FunctionScoreQuery, Query> functionScoreQueryExtractor = q -> {
        try {
            Field queryField = FunctionScoreQuery.class.getDeclaredField("in");
            queryField.setAccessible(true);
            return (Query) queryField.get(q);
        }
        catch (Exception e) {
            return new MatchAllDocsQuery();
        }
    };

    public static final List<QueryTreeBuilder<? extends Query>> DEFAULT_BUILDERS = CollectionUtils.makeUnmodifiableList(
            new BooleanQueryTreeBuilder(),
            newConjunctionBuilder(PhraseQuery.class,
                    (b, q) -> Arrays.asList(q.getTerms()).stream().map(TermNode::new).collect(Collectors.toList())),
            newFilteringQueryBuilder(ConstantScoreQuery.class, ConstantScoreQuery::getQuery),
            newFilteringQueryBuilder(BoostQuery.class, BoostQuery::getQuery),
            newQueryBuilder(TermQuery.class, q -> new TermNode(new QueryTerm(q.getTerm()))),
            newQueryBuilder(SpanTermQuery.class, q -> new TermNode(new QueryTerm(q.getTerm()))),
            newConjunctionBuilder(SpanNearQuery.class,
                    (b, q) -> Arrays.asList(q.getClauses()).stream().map(b::buildTree).collect(Collectors.toList())),
            newDisjunctionBuilder(SpanOrQuery.class,
                    (b, q) -> Arrays.asList(q.getClauses()).stream().map(b::buildTree).collect(Collectors.toList())),
            newFilteringQueryBuilder(SpanMultiTermQueryWrapper.class, SpanMultiTermQueryWrapper::getWrappedQuery),
            newFilteringQueryBuilder(SpanNotQuery.class, SpanNotQuery::getInclude),
            newFilteringQueryBuilder(BoostedQuery.class, BoostedQuery::getQuery),
            newDisjunctionBuilder(DisjunctionMaxQuery.class,
                    (b, q) -> q.getDisjuncts().stream().map(b::buildTree).collect(Collectors.toList())),
            TermsQueryTreeBuilder.INSTANCE,
            TermInSetQueryTreeBuilder.INSTANCE,
            new QueryTreeBuilder<SpanWithinQuery>(SpanWithinQuery.class) {
                @Override
                public QueryTree buildTree(QueryAnalyzer builder, SpanWithinQuery query) {
                    return ConjunctionNode.build(builder.buildTree(query.getBig()), builder.buildTree(query.getLittle()));
                }
            },
            new QueryTreeBuilder<SpanContainingQuery>(SpanContainingQuery.class) {
                @Override
                public QueryTree buildTree(QueryAnalyzer builder, SpanContainingQuery query) {
                    return ConjunctionNode.build(builder.buildTree(query.getBig()), builder.buildTree(query.getLittle()));
                }
            },
            newFilteringQueryBuilder(SpanBoostQuery.class, SpanBoostQuery::getQuery),
            newFilteringQueryBuilder(FieldMaskingSpanQuery.class, FieldMaskingSpanQuery::getMaskedQuery),
            newFilteringQueryBuilder(SpanPositionCheckQuery.class, SpanPositionCheckQuery::getMatch),
            newFilteringQueryBuilder(FunctionScoreQuery.class, functionScoreQueryExtractor),
            PayloadScoreQueryTreeBuilder.INSTANCE,
            SpanPayloadCheckQueryTreeBuilder.INSTANCE,
            ANY_NODE_BUILDER
    );

    public static <T extends Query> QueryTreeBuilder<T> newFilteringQueryBuilder(Class<T> queryType, Function<T, Query> filter) {
        return new QueryTreeBuilder<T>(queryType) {
            @Override
            public QueryTree buildTree(QueryAnalyzer builder, T query) {
                return builder.buildTree(filter.apply(query));
            }
        };
    }

    public static <T extends Query> QueryTreeBuilder<T> newQueryBuilder(Class<T> queryType, Function<T, QueryTree> nodeBuilder) {
        return new QueryTreeBuilder<T>(queryType) {
            @Override
            public QueryTree buildTree(QueryAnalyzer builder, T query) {
                return nodeBuilder.apply(query);
            }
        };
    }

    public static <T extends Query> QueryTreeBuilder<T>
            newConjunctionBuilder(Class<T> queryType, BiFunction<QueryAnalyzer, T, List<QueryTree>> extractor) {
        return new QueryTreeBuilder<T>(queryType) {
            @Override
            public QueryTree buildTree(QueryAnalyzer builder, T query) {
                return ConjunctionNode.build(extractor.apply(builder, query));
            }
        };
    }

    public static <T extends Query> QueryTreeBuilder<T>
            newDisjunctionBuilder(Class<T> queryType, BiFunction<QueryAnalyzer, T, List<QueryTree>> extractor) {
        return new QueryTreeBuilder<T>(queryType) {
            @Override
            public QueryTree buildTree(QueryAnalyzer builder, T query) {
                return DisjunctionNode.build(extractor.apply(builder, query));
            }
        };
    }

}
