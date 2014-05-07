package uk.co.flax.luwak.termextractor;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;

import java.util.*;

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

public class QueryTermList implements Iterable<QueryTerm> {

    private final List<QueryTerm> terms;

    public QueryTermList(List<QueryTerm> terms) {
        this.terms = terms;
    }

    public QueryTermList(QueryTerm... terms) {
        this(Arrays.asList(terms));
    }

    public static QueryTermList selectBest(List<QueryTermList> termlists) {
        return selectBest(termlists, new DefaultComparator());
    }

    public static QueryTermList selectBest(List<QueryTermList> termlists, Set<String> undesirableFields) {
        return selectBest(termlists, new FieldComparator(undesirableFields));
    }

    public static QueryTermList selectBest(List<QueryTermList> termlists, Comparator<QueryTermList> comparator) {
        Collections.sort(termlists, comparator);
        return Iterables.getFirst(termlists, null);
    }

    @Override
    public Iterator<QueryTerm> iterator() {
        return terms.iterator();
    }

    public static class DefaultComparator implements Comparator<QueryTermList> {

        @Override
        public int compare(QueryTermList o1, QueryTermList o2) {
            int comparison;

            comparison = Integer.compare(o1.countType(QueryTerm.Type.ANY), o2.countType(QueryTerm.Type.ANY));
            if (comparison != 0)
                return comparison;

            comparison = Integer.compare(o1.countType(QueryTerm.Type.WILDCARD), o2.countType(QueryTerm.Type.WILDCARD));
            if (comparison != 0)
                return comparison;

            comparison = Integer.compare(o2.longestTerm(), o1.longestTerm());
            if (comparison != 0)
                return comparison;

            return Integer.compare(o1.length(), o2.length());
        }

    }

    public static class FieldComparator extends DefaultComparator {

        private final Set<String> fields;

        public FieldComparator(Set<String> fields) {
            this.fields = fields;
        }

        @Override
        public int compare(QueryTermList o1, QueryTermList o2) {

            int comparison = Integer.compare(o1.countFields(fields), o2.countFields(fields));
            if (comparison != 0)
                return comparison;

            return super.compare(o1, o2);
        }

    }

    public int length() {
        return terms.size();
    }

    private int countType(QueryTerm.Type type) {
        int c = 0;
        for (QueryTerm term : terms) {
            if (term.type == type) c++;
        }
        return c;
    }

    private int countFields(Set<String> fields) {
        int c = 0;
        for (QueryTerm term : terms) {
            if (fields.contains(term.field))
                c++;
        }
        return c;
    }

    private int longestTerm() {
        int c = -1;
        for (QueryTerm term : terms) {
            c = Math.max(c, term.term.length());
        }
        return c;
    }

    @Override
    public String toString() {
        return Joiner.on(" ").join(terms);
    }
}
