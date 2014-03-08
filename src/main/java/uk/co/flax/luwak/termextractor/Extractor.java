package uk.co.flax.luwak.termextractor;

import org.apache.lucene.search.Query;

import java.util.List;
import uk.co.flax.luwak.ExceptionHandler;

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
 * Abstract base class for extracting terms from a query.
 *
 * Subclasses should pass in their own types as a parameter to super().
 *
 * @param <T> the type of the Query to extract terms from
 */
public abstract class Extractor<T extends Query> {

    public final Class<T> cls;
    protected ExceptionHandler handler;

    protected Extractor(Class<T> cls) {
        this.cls = cls;
    }

    protected Extractor(Class<T> cls, ExceptionHandler handler) {
        this(cls);
        this.handler = handler;
    }

    /**
     * Extract terms from this query, adding them to a list of terms
     * @param query the Query to extract terms from
     * @param terms the List to add the extracted terms to
     * @param extractors a list of Extractors to use if the Extractor needs to recurse into the query tree
     */
    public abstract void extract(T query, List<QueryTerm> terms, List<Extractor<?>> extractors);

    /**
     * Extract terms from a query using the first matching Extractor in a list.
     * @param query the query to extract terms from
     * @param terms the List to add the extracted terms to
     * @param extractors the list of extractors to check
     */
    @SuppressWarnings("unchecked")
    protected static final void extractTerms(Query query, List<QueryTerm> terms, List<Extractor<?>> extractors) {
        int termcount = terms.size();
        for (Extractor extractor : extractors) {
            if (extractor.cls.isAssignableFrom(query.getClass())) {
                extractor.extract(query, terms, extractors);
                if (terms.size() == termcount) {
                    // TODO: Empty queries - ANYTERM token?
                }
                return;
            }
        }
    }


}
