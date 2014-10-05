package uk.co.flax.luwak.termextractor.weights;

import java.util.Set;

import com.google.common.collect.Sets;
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
 * Weight a set of terms by a scale factor
 */
public class TermWeightNorm extends WeightNorm {

    private final float k;
    private final Set<String> terms;

    /**
     * Create a new TermWeightNorm
     * @param k the scale factor
     * @param terms the terms to weight
     */
    public TermWeightNorm(float k, Set<String> terms) {
        this.k = k;
        this.terms = terms;
    }

    /**
     * Create a new TermWeightNorm
     * @param k the scale factor
     * @param terms the terms to weight
     */
    public TermWeightNorm(float k, String... terms) {
        this(k, Sets.newHashSet(terms));
    }

    @Override
    public float norm(QueryTerm term) {
        if (this.terms.contains(term.term))
            return k;
        return 1;
    }
}
