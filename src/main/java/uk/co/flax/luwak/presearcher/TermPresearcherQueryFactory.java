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
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.Query;
import uk.co.flax.luwak.InputDocument;
import uk.co.flax.luwak.PresearcherQueryFactory;

import java.io.IOException;

public class TermPresearcherQueryFactory implements PresearcherQueryFactory {

    @Override
    public Query buildQuery(InputDocument doc) {
        AtomicReader reader = doc.asAtomicReader();
        try {
            return TermsEnumBooleanQuery.createFrom(reader);
        }
        catch (IOException e) {
            // We're a MemoryIndex, so this shouldn't happen...
            throw new RuntimeException(e);
        }
    }
}
