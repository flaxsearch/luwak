package uk.co.flax.luwak.termextractor.weights;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
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
 * TermWeightor that uses a set of {@link WeightRule}s to determine the score for a list
 * of {@link QueryTerm}s.
 *
 * Each CompoundRuleWeightor uses a {@link uk.co.flax.luwak.termextractor.weights.LengthNorm}, a
 * {@link uk.co.flax.luwak.termextractor.weights.TermTypeNorm} and a
 * {@link uk.co.flax.luwak.termextractor.weights.TokenLengthNorm}.  Other rules can be added,
 * and the parameters to these various rules set using a
 * {@link uk.co.flax.luwak.termextractor.weights.CompoundRuleWeightor.Builder}
 */
public class CompoundRuleWeightor extends TermWeightor {
    
    private final List<WeightRule> rules;
    private final WeightCombiner combiner;

    private CompoundRuleWeightor(List<WeightRule> rules, WeightCombiner combiner) {
        this.rules = rules;
        this.combiner = combiner;
    }

    @Override
    public float weigh(QueryTerm term) {
        float product = 1;
        for (WeightRule rule : rules) {
            product *= rule.weigh(term);
        }
        return product;
    }

    @Override
    protected float combineSubWeights(float[] weights) {
        return this.combiner.combineWeights(weights);
    }

    /**
     * Returns the value of a * e^(-k * x)
     */
    public static float exp(float a, float k, float x) {
        return (float) (a * Math.exp(-k * x));
    }

    public static Builder newWeightor() {
        return new Builder();
    }

    public static final CompoundRuleWeightor DEFAULT_WEIGHTOR = newWeightor().build();

    public static class Builder {

        float type_weight = 0.75f;
        float tok_length_a = 3;
        float tok_length_k = 0.3f;

        List<WeightRule> rules = new ArrayList<>();
        WeightCombiner combiner = new MinWeightCombiner();

        /** Use this parameter for the TermTypeNorm */
        public Builder withAnyTypeWeight(float weight) {
            type_weight = weight;
            return this;
        }

        /** Use these parameters for the token length norm */
        public Builder withTokenLengthNorm(float a, float k) {
            tok_length_a = a;
            tok_length_k = k;
            return this;
        }

        /** Use this rule */
        public Builder withRule(WeightRule rule) {
            rules.add(rule);
            return this;
        }

        public Builder withCombiner(WeightCombiner combiner) {
            this.combiner = combiner;
            return this;
        }

        /** Build a new CompoundRuleWeightor with the defined parameters and rules */
        public CompoundRuleWeightor build() {
            List<WeightRule> rules = Lists.newArrayList(
                    Iterables.concat(
                            Lists.newArrayList(
                                    new TermTypeNorm(type_weight),
                                    new TokenLengthNorm(tok_length_a, tok_length_k)
                            ),
                            this.rules
                    )
            );
            return new CompoundRuleWeightor(rules, combiner);
        }

    }
}
