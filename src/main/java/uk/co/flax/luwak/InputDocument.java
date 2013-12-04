package uk.co.flax.luwak;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.memory.MemoryIndex;

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

public class InputDocument {

    private final String id;

    protected final MemoryIndex index = new MemoryIndex(true);

    protected InputDocument(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public MemoryIndex getDocumentIndex() {
        return index;
    }

    public AtomicReader asAtomicReader() {
        return index.createSearcher().getIndexReader().leaves().get(0).reader();
    }

    public static Builder builder(String id) {
        return new Builder(id);
    }

    public static class Builder {

        private final InputDocument doc;

        public Builder(String id) {
            this.doc = new InputDocument(id);
        }

        public Builder addField(String field, String text, Analyzer analyzer) {
            doc.index.addField(field, text, analyzer);
            return this;
        }

        public InputDocument build() {
            return doc;
        }
    }
}
