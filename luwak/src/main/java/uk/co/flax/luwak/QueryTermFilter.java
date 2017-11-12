package uk.co.flax.luwak;

/*
 *   Copyright (c) 2015 Lemur Consulting Ltd.
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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.*;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;

/**
 * Class for recording terms stored in the query index.
 *
 * An instance of QueryTermFilter is passed to {@link Presearcher#buildQuery(LeafReader, QueryTermFilter)},
 * and can be used to restrict the presearcher's disjunction query to terms in the index.
 *
 * @see uk.co.flax.luwak.analysis.BytesRefFilteredTokenFilter
 */
public class QueryTermFilter {

    private final Map<String, BytesRefHash> termsHash = new HashMap<>();

    /**
     * Create a QueryTermFilter for an IndexReader
     * @param reader the {@link IndexReader}
     * @throws IOException on error
     */
    public QueryTermFilter(IndexReader reader) throws IOException {
        Fields mf = MultiFields.getFields(reader);
        for (String field : mf) {
            BytesRefHash terms = new BytesRefHash();
            Terms t = mf.terms(field);
            if (t != null) {
                TermsEnum te = t.iterator();
                BytesRef term;
                while ((term = te.next()) != null) {
                    terms.add(term);
                }
            }
            termsHash.put(field, terms);
        }

    }

    /**
     * Get a BytesRefHash containing all terms for a particular field
     * @param field the field
     * @return a {@link BytesRefHash} containing all terms for the specified field
     */
    public BytesRefHash getTerms(String field) {
        BytesRefHash existing = termsHash.get(field);
        if (existing != null)
            return existing;

        return new BytesRefHash();
    }
}
