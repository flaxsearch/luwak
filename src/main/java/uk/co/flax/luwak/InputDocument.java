package uk.co.flax.luwak;

import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.Query;

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

public abstract class InputDocument {

    private final String id;
    protected final MemoryIndex index = new MemoryIndex(true);

    public InputDocument(String id) {
        this.id = id;
    }

    public abstract Query getPresearcherQuery();

    public String getId() {
        return id;
    }

    public MemoryIndex getDocumentIndex() {
        return index;
    }

}
