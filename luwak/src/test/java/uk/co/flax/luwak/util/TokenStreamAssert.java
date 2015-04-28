package uk.co.flax.luwak.util;

import java.io.IOException;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;

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

public class TokenStreamAssert extends AbstractAssert<TokenStreamAssert, TokenStream> {

    private final CharTermAttribute termAtt;
    private final PositionIncrementAttribute posIncAtt;
    private final OffsetAttribute offsetAtt;
    private int actualpos;

    protected TokenStreamAssert(TokenStream actual) throws IOException {
        super(actual, TokenStreamAssert.class);
        termAtt = actual.addAttribute(CharTermAttribute.class);
        posIncAtt = actual.addAttribute(PositionIncrementAttribute.class);
        offsetAtt = actual.addAttribute(OffsetAttribute.class);
        actual.reset();
        actualpos = 0;
    }

    public static TokenStreamAssert assertThat(TokenStream actual) throws IOException {
        return new TokenStreamAssert(actual);
    }

    public TokenStreamAssert nextEquals(String token) throws IOException {
        Assertions.assertThat(actual.incrementToken())
                .overridingErrorMessage("Reached end of token stream")
                .isTrue();
        Assertions.assertThat(termAtt.toString()).isEqualTo(token);
        return this;
    }

    public TokenStreamAssert nextEquals(String token, int position) throws IOException {
        Assertions.assertThat(actual.incrementToken())
                .overridingErrorMessage("Reached end of token stream")
                .isTrue();
        Assertions.assertThat(termAtt.toString()).isEqualTo(token);

        actualpos += posIncAtt.getPositionIncrement();
        Assertions.assertThat(actualpos)
                .overridingErrorMessage("Expected position %d at term [%s], got %d",
                        position, token, actualpos)
                .isEqualTo(position);

        return this;
    }

    public TokenStreamAssert nextEquals(String token, int position, int start, int end) throws IOException {
        Assertions.assertThat(actual.incrementToken())
                .overridingErrorMessage("Reached end of token stream")
                .isTrue();
        Assertions.assertThat(termAtt.toString()).isEqualTo(token);

        actualpos += posIncAtt.getPositionIncrement();
        Assertions.assertThat(actualpos)
                .overridingErrorMessage("Expected position %d at term [%s], got %d",
                        position, token, actualpos)
                .isEqualTo(position);

        Assertions.assertThat(offsetAtt.startOffset())
                .overridingErrorMessage("Expected offset start %d at term [%s], got %d",
                        start, token, offsetAtt.startOffset())
                .isEqualTo(start);

        Assertions.assertThat(offsetAtt.endOffset())
                .overridingErrorMessage("Expected offset end %d at term [%s], got %d",
                        end, token, offsetAtt.endOffset())
                .isEqualTo(end);

        return this;
    }

    public TokenStreamAssert isExhausted() throws IOException {
        Assertions.assertThat(actual.incrementToken()).isFalse();
        actual.close();
        return this;
    }

    public TokenStreamAssert skip(int count) throws IOException {
        for (int i = 0; i < count; i++) {
            actual.incrementToken();
        }
        return this;
    }
}
