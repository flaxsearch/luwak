package uk.co.flax.luwak.termextractor.querytree;

import java.util.List;
import java.util.Set;

import uk.co.flax.luwak.termextractor.QueryTerm;
import uk.co.flax.luwak.termextractor.weights.*;

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

public class TreeWeightor {

    protected final WeightPolicy weightPolicy;
    protected final CombinePolicy combinePolicy;

    public TreeWeightor(WeightPolicy weightPolicy, CombinePolicy combinePolicy) {
        this.weightPolicy = weightPolicy;
        this.combinePolicy = combinePolicy;
    }

    public TreeWeightor(CombinePolicy combine, WeightNorm... norms) {
        this(new WeightPolicy.Default(norms), combine);
    }

    public TreeWeightor(WeightNorm... norms) {
        this(new MinWeightCombiner(), norms);
    }

    public TreeWeightor(WeightPolicy weightPolicy) {
        this(weightPolicy, new MinWeightCombiner());
    }

    public static final TreeWeightor DEFAULT_WEIGHTOR
            = new TreeWeightor(new WeightPolicy.Default(new TermTypeNorm(0), new TokenLengthNorm(3, 0.3f)),
                                                        new MinWeightCombiner());

    protected TreeWeightor(TreeWeightor delegate) {
        this(delegate.weightPolicy, delegate.combinePolicy);
    }

    public float combine(List<QueryTree> children) {
        return combinePolicy.combine(children);
    }

    public QueryTree select(Set<QueryTree> children) {
        if (children.size() == 0)
            throw new IllegalArgumentException("Cannot select child from empty list");

        QueryTree selected = null;
        for (QueryTree child : children) {
            if (selected == null)
                selected = child;
            else {
                if (child.weight > selected.weight)
                    selected = child;
            }
        }
        return selected;
    }

    public float weigh(QueryTerm term) {
        return weightPolicy.weigh(term);
    }

}
