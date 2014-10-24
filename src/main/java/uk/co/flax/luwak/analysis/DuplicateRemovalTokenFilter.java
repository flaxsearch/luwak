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

import java.io.IOException;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.FilteringTokenFilter;

/**
 * A TokenFilter that removes tokens that have already been seen in the TokenStream
 */
public class DuplicateRemovalTokenFilter extends FilteringTokenFilter {

    private final CharArraySet seenTerms = new CharArraySet(1024, false);
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

    public DuplicateRemovalTokenFilter(TokenStream input) {
        super(input);
    }

    @Override
    public void reset() throws IOException {
        super.reset();
        seenTerms.clear();
    }

    @Override
    protected boolean accept() throws IOException {
        return seenTerms.add(termAtt.subSequence(0, termAtt.length()));
    }

}
