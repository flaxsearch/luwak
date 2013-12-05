package uk.co.flax.luwak.termextractor;

import com.google.common.base.Splitter;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import org.apache.lucene.search.RegexpQuery;

import java.util.List;
import java.util.regex.Pattern;

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
 * Extracts longest exact substring from a regular expression, to then be matched
 * against ngrams from an input document.
 *
 * N.B. the only regex chars dealt with currently are '.*' and '?'
 */
public class RegexpNGramTermExtractor extends Extractor<RegexpQuery> {

    public RegexpNGramTermExtractor() {
        super(RegexpQuery.class);
    }

    public static Pattern regexpChars = Pattern.compile("\\.\\*|\\?");

    public static Splitter regexpSplitter = Splitter.on(regexpChars);

    /** Orders strings by length, longest first */
    public static Ordering<String> byLengthOrdering = new Ordering<String>() {
        public int compare(String left, String right) {
            return Ints.compare(left.length(), right.length());
        }
    };

    @Override
    public void extract(RegexpQuery query, List<QueryTerm> terms,
                            List<Extractor<?>> extractors) {
        String regexp = parseOutRegexp(query.toString(""));
        for (String substr : byLengthOrdering.greatestOf(regexpSplitter.split(regexp), 1)) {
            terms.add(new QueryTerm(query.getField(), substr, QueryTerm.Type.WILDCARD));
            return;
        }
    }

    /**
     * The only way to extract a regexp from a RegexpQuery is by parsing it's
     * toString(String) output.  Which is a bit rubbish, really...
     * @param queryRepresentation the toString() representation of a RegexpQuery
     * @return the regular expression for the query
     */
    public static String parseOutRegexp(String queryRepresentation) {
        int fieldSepPos = queryRepresentation.indexOf(":");
        int firstSlash = queryRepresentation.indexOf("/", fieldSepPos);
        int lastSlash = queryRepresentation.lastIndexOf("/");
        return queryRepresentation.substring(firstSlash + 1, lastSlash);
    }
}
