package uk.co.flax.luwak.termextractor.weights;

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
public class TermTypeNorm extends WeightNorm {

    private final float weight;
    private final String payload;
    private final QueryTerm.Type type;

    public TermTypeNorm(QueryTerm.Type type, float weight) {
        this(type, null, weight);
    }

    public TermTypeNorm(QueryTerm.Type type, String payload, float weight) {
        this.weight = weight;
        this.type = type;
        this.payload = payload;
    }

    @Override
    public float norm(QueryTerm term) {
        if (term.type == this.type &&
                (term.payload == null ? this.payload == null : term.payload.equals(this.payload)))
            return weight;
        return 1;
    }
}
