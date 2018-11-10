package uk.co.flax.luwak.termextractor.weights;

import uk.co.flax.luwak.termextractor.QueryTerm;

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
 * Weight a token by its length
 */
public class TokenLengthNorm extends WeightNorm {

    private final float[] lengthNorms = new float[32];

    /**
     * Create a new TokenLengthNorm
     *
     * Tokens will be scaled according to the equation a * e^(-k * tokenlength)
     *
     * @param a a
     * @param k k
     */
    public TokenLengthNorm(float a, float k) {
        for (int i = 0; i < 32; i++) {
            lengthNorms[i] = (float) (a * (Math.exp(-k * i)));
        }
    }

    public TokenLengthNorm() {
        this(3, 0.3f);
    }

    @Override
    public float norm(QueryTerm term) {
        if (term.term.bytes().length >= 32) {
            return Integer.MAX_VALUE;
        }
        return 4 - lengthNorms[term.term.bytes().length];
    }
}
