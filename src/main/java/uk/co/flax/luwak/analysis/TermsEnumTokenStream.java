package uk.co.flax.luwak.analysis;

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
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

/**
 * A TokenStream created from a {@link org.apache.lucene.index.TermsEnum}
 */
public class TermsEnumTokenStream extends TokenStream {

    private final TermsEnum termsEnum;
    private final CharTermAttribute charTerm = addAttribute(CharTermAttribute.class);

    /** Create a new TermsEnumTokenStream using a TermsEnum */
    public TermsEnumTokenStream(TermsEnum termsEnum) {
        this.termsEnum = termsEnum;
    }

    @Override
    public final boolean incrementToken() throws IOException {
    	clearAttributes();
    	
        BytesRef bytes = termsEnum.next();
        if (bytes == null)
            return false;
        charTerm.setEmpty();
        charTerm.append(bytes.utf8ToString());
        return true;
    }
}
