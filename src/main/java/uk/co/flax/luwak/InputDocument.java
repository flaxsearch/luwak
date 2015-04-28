package uk.co.flax.luwak;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.IndexSearcher;

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
 * An InputDocument represents a document to be run against registered queries
 * in the Monitor.  It should be constructed using the static #builder() method.
 */
public class InputDocument {

    /**
     * Create a new fluent {@link uk.co.flax.luwak.InputDocument.Builder} object.
     * @param id the id
     * @return a Builder
     */
    public static Builder builder(String id) {
        return new Builder(id);
    }

    private final String id;

    private final MemoryIndex index = new MemoryIndex(true);
    private IndexSearcher searcher;

    // protected constructor - use a Builder to create objects
    protected InputDocument(String id) {
        this.id = id;
    }

    private void finish() {
        index.freeze();
        searcher = index.createSearcher();
    }

    /**
     * Get the document's ID
     * @return the document's ID
     */
    public String getId() {
        return id;
    }

    public IndexSearcher getSearcher() {
        return searcher;
    }

    /**
     * Get an atomic reader over the internal index
     * @return an {@link org.apache.lucene.index.LeafReader} over the internal index
     */
    public LeafReader asAtomicReader() {
        return searcher.getIndexReader().leaves().get(0).reader();
    }

    /**
     * Fluent interface to construct a new InputDocument
     */
    public static class Builder {

        private final InputDocument doc;

        /**
         * Create a new Builder for an InputDocument with the given id
         * @param id the id of the InputDocument
         */
        public Builder(String id) {
            this.doc = new InputDocument(id);
        }

        /**
         * Add a field to the InputDocument
         *
         * @param field the field name
         * @param text the text content of the field
         * @param analyzer the {@link Analyzer} to use for this field
         *
         * @return the Builder object
         */
        public Builder addField(String field, String text, Analyzer analyzer) {
            doc.index.addField(field, text, analyzer);
            return this;
        }

        /**
         * Add a field to the InputDocument
         *
         * @param field the field name
         * @param tokenStream a tokenstream containing the field contents
         *                    
         * @return the Builder object
         */
        public Builder addField(String field, TokenStream tokenStream) {
            doc.index.addField(field, tokenStream);
            return this;
        }

        /**
         * Build the InputDocument
         * @return the InputDocument
         */
        public InputDocument build() {
            doc.finish();
            return doc;
        }

    }

}
