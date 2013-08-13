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

import java.io.IOException;
import java.util.List;

public class StringSetTokenStream extends TokenStream {

    private final List<String> terms;

    public StringSetTokenStream(List<String> terms) {
        this.terms = terms;
    }

    final CharTermAttribute chTerm = addAttribute(CharTermAttribute.class);
    private int index = 0;

    @Override
    public final boolean incrementToken() throws IOException {
        if (index >= terms.size())
            return false;
        chTerm.setEmpty();
        chTerm.append(terms.get(index++));
        return true;
    }
}
