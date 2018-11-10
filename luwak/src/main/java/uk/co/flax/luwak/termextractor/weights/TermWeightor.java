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
 * A combination of {@link WeightNorm} classes that calculates the weight
 * of a {@link QueryTerm}
 *
 * Individual {@link WeightNorm} results are combined by multiplication.
 */
public final class TermWeightor {

    private final WeightNorm[] norms;

    /**
     * Create a new TermWeightor from a combination of WeightNorms
     *
     * Note that passing an empty list will result in all terms being given
     * a weight of {@code 1f}
     */
    public TermWeightor(WeightNorm... norms) {
        this.norms = norms;
    }

    /**
     * Calculates the weight of a specific term
     */
    public float weigh(QueryTerm term) {
        float termweight = 1;
        for (WeightNorm norm : norms) {
            termweight *= norm.norm(term);
        }
        return termweight;
    }

}
