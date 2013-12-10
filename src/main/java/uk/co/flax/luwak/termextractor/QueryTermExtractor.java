package uk.co.flax.luwak.termextractor;/*
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

import com.google.common.collect.ImmutableList;
import org.apache.lucene.search.Query;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Utility class to extract terms from a {@link Query} by walking the query tree.
 *
 * Extracted terms may then be used to store queries in the Monitor.
 */
public class QueryTermExtractor {

    private final String ANYTOKEN = "__ANYTOKEN__";

    private final List<Extractor<?>> extractors = new ArrayList<>();

    /**
     * The default list of Extractors to use
     */
    public static final ImmutableList<Extractor<?>> DEFAULT_EXTRACTORS = ImmutableList.of(
            new IntervalFilterQueryExtractor(),
            new NonOverlappingQueryExtractor(),
            new FieldedConjunctionQueryExtractor(),
            new FieldedBooleanQueryExtractor(),
            new BooleanTermExtractor(),
            new NumericRangeExtractor(),
            new RegexpAnyTermExtractor(),
            new SimpleTermExtractor(),
            new GenericTermExtractor()
    );

    /**
     * Create a new QueryTermExtractor using the default {@link Extractor} list
     */
    public QueryTermExtractor() {
        extractors.addAll(DEFAULT_EXTRACTORS);
    }

    /**
     * Create a new QueryTermExtractor using user-specifed {@link Extractor}s, in
     * addition to the default list.
     *
     * Extractors passed in here will override a default defined on the same query type.
     *
     * @param extractors an array of Extractors
     */
    public QueryTermExtractor(Extractor<?>... extractors) {
        for (Extractor<?> extractor : extractors) {
            this.extractors.add(extractor);
        }
        this.extractors.addAll(DEFAULT_EXTRACTORS);
    }

    /**
     * Extract terms from a query
     * @param query the query to extract terms from
     * @return a Set of {@link QueryTerm}s
     */
    public final Set<QueryTerm> extract(Query query) {
        List<QueryTerm> terms = new ArrayList<>();
        Extractor.extractTerms(query, terms, extractors);
        return new HashSet<>(terms);
    }

    /**
     * Get the token used to match all documents
     * @return the AnyToken
     */
    public String getAnyToken() {
        return ANYTOKEN;
    }
}
