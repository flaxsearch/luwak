package uk.co.flax.luwak.analysis;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.CodepointCountFilter;
import org.apache.lucene.analysis.tokenattributes.*;
import org.apache.lucene.analysis.util.CharacterUtils;
import uk.co.flax.luwak.Constants;

import java.io.IOException;

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

public final class SuffixingNGramTokenFilter extends TokenFilter {

    private final int minGram, maxGram;
    private final String suffix;
    private final int maxTokenLength;
    private final String anyToken;

    private char[] curTermBuffer;
    private int curTermLength;
    private int curCodePointCount;
    private int curGramSize;
    private int curPos;
    private int curPosInc, curPosLen;
    private int tokStart;
    private int tokEnd;

    private final CharacterUtils charUtils;
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final PositionIncrementAttribute posIncAtt;
    private final PositionLengthAttribute posLenAtt;
    private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
    private final KeywordAttribute keywordAtt = addAttribute(KeywordAttribute.class);

    /**
     * Creates NGramTokenFilter with given min and max n-grams.
     * @param input {@link org.apache.lucene.analysis.TokenStream} holding the input to be tokenized
     * @param suffix a string to suffix to all ngrams
     */
    public SuffixingNGramTokenFilter(TokenStream input, String suffix, String anyToken, int maxTokenLength) {
        super(new CodepointCountFilter(Constants.VERSION, input, 1, Integer.MAX_VALUE));
        this.charUtils = CharacterUtils.getInstance(Constants.VERSION);

        this.minGram = 1;
        this.maxGram = Integer.MAX_VALUE;
        this.suffix = suffix;
        this.anyToken = anyToken;
        this.maxTokenLength = maxTokenLength;

        posIncAtt = addAttribute(PositionIncrementAttribute.class);
        posLenAtt = addAttribute(PositionLengthAttribute.class);

    }

    /** Returns the next token in the stream, or null at EOS. */
    @Override
    public final boolean incrementToken() throws IOException {
        while (true) {
            if (curTermBuffer == null) {

                if (!input.incrementToken()) {
                    return false;
                }

                if (keywordAtt.isKeyword())
                    return true;

                if (termAtt.length() > maxTokenLength) {
                    termAtt.setEmpty().append(anyToken);
                    return true;
                }

                curTermBuffer = termAtt.buffer().clone();
                curTermLength = termAtt.length();
                curCodePointCount = charUtils.codePointCount(termAtt);
                curGramSize = minGram;
                curPos = 0;
                curPosInc = posIncAtt.getPositionIncrement();
                curPosLen = posLenAtt.getPositionLength();
                tokStart = offsetAtt.startOffset();
                tokEnd = offsetAtt.endOffset();
                termAtt.setEmpty().append(suffix);
                return true;

            }

            if (curGramSize > maxGram || (curPos + curGramSize) > curCodePointCount) {
                ++curPos;
                curGramSize = minGram;
            }
            if ((curPos + curGramSize) <= curCodePointCount) {
                clearAttributes();
                final int start = charUtils.offsetByCodePoints(curTermBuffer, 0, curTermLength, 0, curPos);
                final int end = charUtils.offsetByCodePoints(curTermBuffer, 0, curTermLength, start, curGramSize);
                termAtt.copyBuffer(curTermBuffer, start, end - start);
                termAtt.append(suffix);
                posIncAtt.setPositionIncrement(curPosInc);
                curPosInc = 0;
                posLenAtt.setPositionLength(curPosLen);
                offsetAtt.setOffset(tokStart, tokEnd);
                curGramSize++;
                return true;
            }

            curTermBuffer = null;
        }
    }

    @Override
    public void reset() throws IOException {
        super.reset();
        curTermBuffer = null;
    }
}