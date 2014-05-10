package uk.co.flax.luwak.termextractor.weights;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import uk.co.flax.luwak.termextractor.QueryTerm;

import java.util.ArrayList;
import java.util.List;

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
public class CompoundRuleWeightor implements TermWeightor {
    
    private final List<WeightRule> rules;

    private CompoundRuleWeightor(List<WeightRule> rules) {
        this.rules = rules;
    }

    @Override
    public float weigh(List<QueryTerm> terms) {
        float product = 1;
        for (WeightRule rule : rules) {
            product *= rule.weigh(terms);
        }
        return product;
    }

    public static float exp(float a, float k, float x) {
        return (float) (a * Math.exp(-k * x));
    }

    public static Builder newWeightor() {
        return new Builder();
    }

    public static final CompoundRuleWeightor DEFAULT_WEIGHTOR = newWeightor().build();

    public static class Builder {

        float length_a = 3;
        float length_k = 0.3f;
        float type_weight = 0.75f;
        float tok_length_a = 3;
        float tok_length_k = 0.3f;

        List<WeightRule> rules = new ArrayList<>();

        public Builder withListLengthNorm(float a, float k) {
            length_a = a;
            length_k = k;
            return this;
        }

        public Builder withAnyTypeWeight(float weight) {
            type_weight = weight;
            return this;
        }

        public Builder withTokenLengthNorm(float a, float k) {
            tok_length_a = a;
            tok_length_k = k;
            return this;
        }

        public Builder withRule(WeightRule rule) {
            rules.add(rule);
            return this;
        }

        public CompoundRuleWeightor build() {
            List<WeightRule> rules = Lists.newArrayList(
                    Iterables.concat(
                            Lists.newArrayList(
                                    new LengthNorm(length_a, length_k),
                                    new TermTypeNorm(type_weight),
                                    new TokenLengthNorm(tok_length_a, tok_length_k)
                            ),
                            this.rules
                    )
            );
            return new CompoundRuleWeightor(rules);
        }

    }
}
