package uk.co.flax.luwak.util;

/*
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
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

import java.io.IOException;

/**
 * Create a disjunction query from all values in a TokenStream
 */
public class TokenStreamBooleanQuery {

    static {
        BooleanQuery.setMaxClauseCount(10000);
    }

    /**
     * Create a new Query from a TokenStream
     * @param field the field to create the query for
     * @param ts the TokenStream
     * @return a BooleanQuery using all values from the TokenStream
     * @throws IOException if an exception is encountered iterating over the TokenStream
     */
    public static Query fromTokenStream(String field, TokenStream ts) throws IOException {

        BooleanQuery bq = new BooleanQuery();

        final CharTermAttribute cht = ts.addAttribute(CharTermAttribute.class);
        while (ts.incrementToken()) {
            bq.add(new TermQuery(new Term(field, cht.toString())), BooleanClause.Occur.SHOULD);
        }

        return bq;
    }
}
