package uk.co.flax.luwak.analysis;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.IOException;
import java.util.Set;

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

/**
 * A TokenFilter that filters out tokens not found in a whitelist
 */
public final class WhitelistTokenFilter extends TokenFilter {

    private final Set<String> terms;

    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

    /**
     * Create a new WhitelistTokenFilter
     * @param ts the TokenStream to filter
     * @param terms a Set of terms to allow through
     */
    public WhitelistTokenFilter(TokenStream ts, Set<String> terms) {
        super(ts);
        this.terms = terms;
    }

    @Override
    public boolean incrementToken() throws IOException {
        while (true) {
            if (!input.incrementToken())
                return false;
            if (terms.contains(termAtt.toString()))
                return true;
        }
    }
}
