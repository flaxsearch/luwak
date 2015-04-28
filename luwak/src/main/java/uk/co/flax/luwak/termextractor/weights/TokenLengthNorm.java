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

    private final float a;
    private final float k;

    /**
     * Create a new TokenLengthNorm
     *
     * Tokens will be scaled according to the equation a * e^(-k * tokenlength)
     *
     * @param a a
     * @param k k
     */
    public TokenLengthNorm(float a, float k) {
        this.a = a;
        this.k = k;
    }

    @Override
    public float norm(QueryTerm term) {
        if (term.type == QueryTerm.Type.ANY)
            return 1;
        else
            return (4 - WeightNorm.exp(a, k, term.term.length()));
    }
}
