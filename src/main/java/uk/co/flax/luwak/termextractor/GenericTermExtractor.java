package uk.co.flax.luwak.termextractor;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
 * Extracts terms from a generic Query using {@link Query#extractTerms(java.util.Set)}
 */
public class GenericTermExtractor extends Extractor<Query> {

    public GenericTermExtractor() {
        super(Query.class);
    }

    @Override
    public void extract(Query query, List<QueryTerm> terms, List<Extractor<?>> extractors) {
        Set<Term> termSet = new HashSet<>();
        try {
            query.extractTerms(termSet);
            for (Term term : termSet) {
                terms.add(new QueryTerm(term.field(), term.text(), QueryTerm.Type.EXACT));
            }
        }
        catch (UnsupportedOperationException e) {
            if (handler == null) {
                throw new RuntimeException(new RuntimeException("Cannot extract terms from query of type " + query.getClass()));
            } else {
                handler.exception(e);
            }
        }
    }
}
