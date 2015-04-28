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

    /** The payload */
    public final String payload;

    /** Construct a new QueryTerm */
    public QueryTerm(String field, String term, Type type, String payload) {
        this.field = field;
        this.term = term;
        this.type = type;
        this.payload = payload;
    }

    public QueryTerm(String field, String term, Type type) {
        this(field, term, type, null);
    }

    public QueryTerm(Term term) {
        this(term.field(), term.text(), Type.EXACT);
    }

    public enum Type { EXACT, ANY, CUSTOM }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        QueryTerm queryTerm = (QueryTerm) o;

        if (field != null ? !field.equals(queryTerm.field) : queryTerm.field != null) return false;
        if (term != null ? !term.equals(queryTerm.term) : queryTerm.term != null) return false;
        if (type != null ? !type.equals(queryTerm.type) : queryTerm.type != null) return false;
        if (payload != null ? !payload.equals(queryTerm.payload) : queryTerm.payload != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = field != null ? field.hashCode() : 0;
        result = 31 * result + (term != null ? term.hashCode() : 0);
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (payload != null ? payload.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return type + " " + field + ":[" + term + (payload == null ? "]" : "]{" + payload + "}");
    }
}
