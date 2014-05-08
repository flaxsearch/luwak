package uk.co.flax.luwak.presearcher;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.EmptyTokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.FilteringTokenFilter;
import org.apache.lucene.util.Version;

import java.io.IOException;
import java.util.Set;

/**
* Copyright (c) 2014 Lemur Consulting Ltd.
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
public interface DocumentTokenFilter {

    public TokenStream filter(String field, TokenStream in) throws IOException;

    public static final DocumentTokenFilter PASSTHROUGH = new DocumentTokenFilter() {
        @Override
        public TokenStream filter(String field, TokenStream in) {
            return in;
        }
    };

    public static class FieldFilter implements DocumentTokenFilter {

        private final String field;

        public FieldFilter(String field) {
            this.field = field;
        }

        @Override
        public TokenStream filter(String field, TokenStream in) {
            if (this.field.equals(field))
                return new EmptyTokenStream();
            return in;
        }
    }

    public static class TokensFilter implements DocumentTokenFilter {

        private final CharArraySet tokensToFilter = new CharArraySet(Version.LUCENE_50, 1024, false);

        public TokensFilter(Set<String> tokensToFilter) {
            this.tokensToFilter.addAll(tokensToFilter);
        }

        @Override
        public TokenStream filter(String field, TokenStream in) {
            return new FilteringTokenFilter(Version.LUCENE_50, in) {

                CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

                @Override
                protected boolean accept() throws IOException {
                    return !tokensToFilter.contains(termAtt);
                }
            };
        }
    }

    public static class FieldTokensFilter extends TokensFilter {

        private final String field;

        public FieldTokensFilter(String field, Set<String> tokensToFilter) {
            super(tokensToFilter);
            this.field = field;
        }

        @Override
        public TokenStream filter(String field, TokenStream in) {
            if (!this.field.equals(field))
                return in;
            return super.filter(field, in);
        }
    }

}
