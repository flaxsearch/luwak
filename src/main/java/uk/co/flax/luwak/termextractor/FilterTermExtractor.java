package uk.co.flax.luwak.termextractor;

import java.util.*;

import com.google.common.collect.ImmutableList;
import org.apache.lucene.search.Filter;

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

public class FilterTermExtractor {

    /**
     * The default list of Extractors to use
     */
    public static final List<FilterExtractor<? extends Filter>> DEFAULT_FILTER_EXTRACTORS = ImmutableList.of(
            new TermsFilterTermExtractor(),
            new TermFilterTermExtractor(),
            new GenericFilterTermExtractor()
    );

    protected final List<FilterExtractor<? extends Filter>> filterExtractors;

    public FilterTermExtractor(FilterExtractor<? extends Filter>... extractors) {
        filterExtractors = new LinkedList<>();
        if (extractors != null)
            filterExtractors.addAll(Arrays.asList(extractors));
        filterExtractors.addAll(DEFAULT_FILTER_EXTRACTORS);
    }

    public final List<QueryTerm> extract(Filter filter) {
        return FilterExtractor.extractTerms(filter, filterExtractors);
    }
}
