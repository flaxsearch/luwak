package uk.co.flax.luwak.termextractor;

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

import org.apache.lucene.index.Term;

/**
 * Represents information about an extracted term
 */
public class QueryTerm {

    /** The field of this term */
    public final String field;

    /** The term value */
    public final String term;

    /** The term type */
    public final Type type;

    /** Construct a new QueryTerm */
    public QueryTerm(String field, String term, Type type) {
        this.field = field;
        this.term = term;
        this.type = type;
    }

    public QueryTerm(Term term) {
        this(term.field(), term.text(), Type.EXACT);
    }

    /**
     * Type of a term
     */
    public static final class Type {

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Type type1 = (Type) o;

            if (payload != null ? !payload.equals(type1.payload) : type1.payload != null) return false;
            if (type != type1.type) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = payload != null ? payload.hashCode() : 0;
            result = 31 * result + type.hashCode();
            return result;
        }

        public enum T {
            /** Queries will match against the exact term */
            EXACT,

            /** The term will match any document */
            ANY,

            /** Custom type */
            CUSTOM
        }

        public final String payload;
        public final T type;

        private Type(T type, String payload) {
            this.type = type;
            this.payload = payload;
        }

        public static Type CUSTOM(String payload) {
            return new Type(T.CUSTOM, payload);
        }

        public static final Type EXACT = new Type(T.EXACT, null);

        public static final Type ANY = new Type(T.ANY, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        QueryTerm queryTerm = (QueryTerm) o;

        if (field != null ? !field.equals(queryTerm.field) : queryTerm.field != null) return false;
        if (term != null ? !term.equals(queryTerm.term) : queryTerm.term != null) return false;
        if (type != null ? !type.equals(queryTerm.type) : queryTerm.type != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = field != null ? field.hashCode() : 0;
        result = 31 * result + (term != null ? term.hashCode() : 0);
        result = 31 * result + (type != null ? type.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "field: " + field + ", term: " + term + " [" + type + "]";
    }
}
