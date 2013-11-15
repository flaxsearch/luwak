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

public class QueryTermExtractor {

    private final List<Extractor<?>> extractors = new ArrayList<>();

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

    public QueryTermExtractor() {
        extractors.addAll(DEFAULT_EXTRACTORS);
    }

    public QueryTermExtractor(Extractor<?>... extractors) {
        for (Extractor<?> extractor : extractors) {
            this.extractors.add(extractor);
        }
        this.extractors.addAll(DEFAULT_EXTRACTORS);
    }

    public final Set<QueryTerm> extract(Query query) {
        List<QueryTerm> terms = new ArrayList<>();
        extractTerms(query, terms);
        return new HashSet<>(terms);
    }

    @SuppressWarnings("unchecked")
    public final void extractTerms(Query query, List<QueryTerm> terms) {
        int termcount = terms.size();
        for (Extractor extractor : extractors) {
            if (extractor.cls.isAssignableFrom(query.getClass())) {
                extractor.extract(query, terms, this);
                if (terms.size() == termcount) {
                    // TODO: Empty queries - ANYTERM token?
                }
                return;
            }
        }
    }

}
