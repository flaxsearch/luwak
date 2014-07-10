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

import java.util.*;

import com.google.common.collect.ImmutableList;
import org.apache.lucene.search.Query;
import uk.co.flax.luwak.termextractor.extractors.*;
import uk.co.flax.luwak.termextractor.weights.CompoundRuleWeightor;
import uk.co.flax.luwak.termextractor.weights.TermWeightor;

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
            new NumericRangeExtractor(),
            new RegexpAnyTermExtractor(),
            new SimpleTermExtractor(),
            new FilteredQueryExtractor(),
            new TermsFilterTermExtractor(),
            new TermFilterTermExtractor(),
            new GenericFilterTermExtractor(),
            new GenericTermExtractor()
    );

    public QueryTermExtractor(Extractor<?>... extractors) {
        this(CompoundRuleWeightor.DEFAULT_WEIGHTOR, extractors);
    }

    /**
     * Create a new QueryTermExtractor using user-specifed {@link Extractor}s, in
     * addition to the default list.
     *
     * Extractors passed in here will override a default defined on the same query type.
     *
     * @param weightor   the {@link uk.co.flax.luwak.termextractor.weights.TermWeightor} to use for Boolean clauses
     * @param extractors an array of Extractors
     */
    public QueryTermExtractor(TermWeightor weightor, Extractor<?>... extractors) {
        Collections.addAll(this.extractors, extractors);
        this.extractors.add(new BooleanTermExtractor.QueryExtractor(weightor));
        this.extractors.add(new BooleanTermExtractor.FilterExtractor(weightor));
        this.extractors.add(new PhraseQueryTermExtractor(weightor));
        this.extractors.add(new ConstantScoreQueryExtractor());
        this.extractors.addAll(DEFAULT_EXTRACTORS);
    }

    /**
     * Extract terms from a query
     * @param query the query to extract terms from
     * @return a Set of {@link QueryTerm}s
     */
    public final Set<QueryTerm> extract(Object query) {
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
