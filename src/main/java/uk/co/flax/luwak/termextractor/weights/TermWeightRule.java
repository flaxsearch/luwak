package uk.co.flax.luwak.termextractor.weights;

import uk.co.flax.luwak.termextractor.QueryTerm;

import java.util.List;
import java.util.Map;

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
public class TermWeightRule implements WeightRule {

    public TermWeightRule(Map<String, Float> terms) {
        this.terms = terms;
    }

    private final Map<String, Float> terms;

    @Override
    public float weigh(List<QueryTerm> terms) {
        float result = 1;
        for (QueryTerm term : terms) {
            if (this.terms.containsKey(term.term))
                result *= this.terms.get(term.term);
        }
        return result;
    }

}
