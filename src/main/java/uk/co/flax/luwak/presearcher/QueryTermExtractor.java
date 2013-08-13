package uk.co.flax.luwak.presearcher;/*
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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;

import java.io.IOException;
import java.util.*;

public class QueryTermExtractor {

    private final Map<String, List<String>> terms = new HashMap<>();

    public QueryTermExtractor(Query query) {
        Set<Term> qterms = new HashSet<>();
        query.extractTerms(qterms);
        for (Term term : qterms) {
            if (!terms.containsKey(term.field()))
                terms.put(term.field(), new ArrayList<String>());
            terms.get(term.field()).add(term.text());
        }
    }

    public Set<String> getFields() {
        return terms.keySet();
    }

    public TokenStream getTokenStream(final String field) {
        return new TokenStream() {

            CharTermAttribute chTerm = addAttribute(CharTermAttribute.class);
            int index = 0;

            @Override
            public boolean incrementToken() throws IOException {
                if (index >= terms.get(field).size())
                    return false;
                chTerm.setEmpty();
                chTerm.append(terms.get(field).get(index++));
                return true;
            }
        };
    }
}
