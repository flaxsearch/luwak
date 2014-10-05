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
 * Weight a set of fields by a scale factor
 */
public class FieldWeightNorm extends WeightNorm {

    private final Set<String> fields;
    private final float k;

    /**
     * Create a new FieldWeightNorm
     * @param k the scale factor
     * @param fields the fields to scale
     */
    public FieldWeightNorm(float k, Set<String> fields) {
        this.fields = fields;
        this.k = k;
    }

    /**
     * Create a new FieldWeightNorm
     * @param k the scale factor
     * @param fields the fields to scale
     */
    public FieldWeightNorm(float k, String... fields) {
        this(k, Sets.newHashSet(fields));
    }

    @Override
    public float norm(QueryTerm term) {
        if (this.fields.contains(term.field))
            return k;
        return 1;
    }
}
