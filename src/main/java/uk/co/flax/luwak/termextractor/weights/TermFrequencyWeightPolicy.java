package uk.co.flax.luwak.termextractor.weights;

import java.util.Map;

import uk.co.flax.luwak.termextractor.QueryTerm;

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

/**
 * Weights more infrequent terms more highly
 */
public class TermFrequencyWeightPolicy extends WeightPolicy {

    final Map<String, Integer> frequencies;
    final float n;
    final float k;

    /**
     * Creates a TermFrequencyNorm
     * @param frequencies map of terms to term frequencies
     * @param n scaling factor to use for frequencies
     * @param k minimum weight to scale to
     * @param norms WeightNorms to use for further normalization
     */
    public TermFrequencyWeightPolicy(Map<String, Integer> frequencies, float n, float k, WeightNorm... norms) {
        super(norms);
        this.frequencies = frequencies;
        this.n = n;
        this.k = k;
    }

    @Override
    public float weighTerm(QueryTerm term) {
        if (this.frequencies.containsKey(term.term))
            return (n / this.frequencies.get(term.term)) + k;
        return 1;
    }
}
