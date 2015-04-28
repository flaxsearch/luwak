package uk.co.flax.luwak.analysis;

import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;

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

public class OrderedTokenFilter extends TokenFilter {

    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final PositionIncrementAttribute posAtt = addAttribute(PositionIncrementAttribute.class);

    private final CharTermAttribute inputAtt;

    private final PriorityQueue<String> tokens = new PriorityQueue<>();

    private boolean done = false;

    public OrderedTokenFilter(TokenStream in) {
        super(in);
        inputAtt = in.addAttribute(CharTermAttribute.class);
    }

    @Override
    public final boolean incrementToken() throws IOException {
        if (done)
            return false;

        if (tokens.peek() == null) {
            while (input.incrementToken()) {
                tokens.add(inputAtt.toString());
            }
        }

        String nextToken = tokens.poll();
        if (tokens.peek() == null)
            done = true;

        termAtt.setEmpty().append(nextToken);
        posAtt.setPositionIncrement(0);
        return true;
    }

    @Override
    public void reset() throws IOException {
        done = false;
        tokens.clear();
        super.reset();
    }
}
