package uk.co.flax.luwak.termextractor.weights;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

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
 * Scores a list of queryterms.  Used by a {@link uk.co.flax.luwak.termextractor.treebuilder.BooleanQueryTreeBuilder} to
 * determine which clauses should be indexed.
 */
public abstract class TermWeightor {

    protected abstract float weigh(QueryTerm term);

    protected abstract float combineSubWeights(float[] weights);

    public class WeightedTermsList {
        private final float weight;
        private final List<QueryTerm> terms;

        WeightedTermsList(List<QueryTerm> terms) {
            this.terms = terms;

            if (terms.size() > 0) {
                float[] weights = new float[terms.size()];
                for (int i = 0; i < terms.size(); i++) {
                    weights[i] = weigh(terms.get(i));
                }
                this.weight = combineSubWeights(weights);
            }
            else {
                this.weight = 0;
            }
        }

        @Override
        public String toString() {
            return weight + ": " + terms.toString();
        }
    }

    public List<QueryTerm> selectBest(List<List<QueryTerm>> termlists) {
        List<WeightedTermsList> weightedTerms = new ArrayList<>(termlists.size());
        for (List<QueryTerm> terms : termlists) {
            weightedTerms.add(new WeightedTermsList(terms));
        }
        return selectWeighted(weightedTerms);
    }

    protected List<QueryTerm> selectWeighted(List<WeightedTermsList> weightedTerms) {
        Collections.sort(weightedTerms, new Comparator<WeightedTermsList>() {
            @Override
            public int compare(WeightedTermsList o1, WeightedTermsList o2) {
                return Float.compare(o2.weight, o1.weight);
            }
        });
        return weightedTerms.get(0).terms;
    }
}
