package uk.co.flax.luwak.presearcher;

import java.io.Closeable;
import java.io.IOException;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.EmptyTokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.FilteringTokenFilter;
import org.apache.lucene.index.*;
import org.apache.lucene.util.BytesRefBuilder;

/*
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

/**
 * A PerFieldTokenFilter that filters out all tokens that do not already appear
 * in an index.  Used by presearchers to reduce the size of the queries produced
 * by buildQuery().
 */
public class TermsEnumFilter implements PerFieldTokenFilter, Closeable {

    private final LeafReader reader;

    public TermsEnumFilter(IndexWriter writer) throws IOException {
        this.reader = SlowCompositeReaderWrapper.wrap(DirectoryReader.open(writer, true));
    }

    @Override
    public TokenStream filter(String field, TokenStream in) throws IOException {
        Fields fields = reader.fields();
        if (fields == null)
            return new EmptyTokenStream();
        Terms terms = fields.terms(field);
        if (terms == null)
            return new EmptyTokenStream();
        return new Filter(in, terms.iterator(null));
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    public static final class Filter extends FilteringTokenFilter {

        private final TermsEnum terms;
        private final BytesRefBuilder scratch = new BytesRefBuilder();

        private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

        public Filter(TokenStream in, TermsEnum terms) {
            super(in);
            this.terms = terms;
        }

        @Override
        protected boolean accept() throws IOException {
            scratch.copyChars(termAtt);
            return terms.seekExact(scratch.get());
        }
    }
}
