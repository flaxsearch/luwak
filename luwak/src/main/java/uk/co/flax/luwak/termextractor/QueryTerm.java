package uk.co.flax.luwak.termextractor;

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

import java.util.Objects;

import org.apache.lucene.index.Term;

/**
 * Represents information about an extracted term
 */
public class QueryTerm {

    public final Term term;

    /** The term type */
    public final Type type;

    /** The payload */
    public final String payload;

    /**
     * Construct a new QueryTerm
     *
     * @param term  the term (must not be null)
     * @param type  the {@link uk.co.flax.luwak.termextractor.QueryTerm.Type} (must not be null)
     * @param payload extra information
     */
    public QueryTerm(Term term, Type type, String payload) {
        this.term = Objects.requireNonNull(term);
        this.type = Objects.requireNonNull(type);
        this.payload = payload;
    }

    /**
     * Construct a new QueryTerm
     * @param field the field
     * @param term the term
     * @param type the {@link uk.co.flax.luwak.termextractor.QueryTerm.Type}
     * @param payload extra information
     */
    public QueryTerm(String field, String term, Type type, String payload) {
        this(new Term(field, term), type, payload);
    }

    /**
     * Construct a new QueryTerm
     * @param field the field
     * @param term  the term
     * @param type  the {@link uk.co.flax.luwak.termextractor.QueryTerm.Type}
     */
    public QueryTerm(String field, String term, Type type) {
        this(new Term(field, term), type, null);
    }

    /**
     * Construct a new QueryTerm from a {@link Term}
     * @param term the term
     */
    public QueryTerm(Term term) {
        this(term.field(), term.text(), Type.EXACT);
    }

    public enum Type { EXACT, ANY, CUSTOM }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryTerm queryTerm = (QueryTerm) o;
        return Objects.equals(term, queryTerm.term) &&
                type == queryTerm.type &&
                Objects.equals(payload, queryTerm.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(term, type, payload);
    }

    @Override
    public String toString() {
        return type + " " + term.toString() + (payload == null ? "" : "{" + payload + "}");
    }
}
