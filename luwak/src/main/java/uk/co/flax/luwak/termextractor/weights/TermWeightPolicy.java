package uk.co.flax.luwak.termextractor.weights;

import java.util.Map;

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
public class TermWeightPolicy extends WeightPolicy {

    public TermWeightPolicy(Map<String, Float> terms, WeightNorm... norms) {
        super(norms);
        this.terms = terms;
    }

    private final Map<String, Float> terms;

    @Override
    public float weighTerm(QueryTerm term) {
        if (this.terms.containsKey(term.term.text()))
            return this.terms.get(term.term.text());
        return 1;
    }

}
