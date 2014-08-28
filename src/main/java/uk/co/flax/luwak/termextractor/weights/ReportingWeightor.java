package uk.co.flax.luwak.termextractor.weights;

import java.util.List;

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
public class ReportingWeightor extends TermWeightor {

    private final TermWeightor delegate;
    private final Reporter reporter;

    public ReportingWeightor(Reporter reporter, TermWeightor delegate) {
        this.delegate = delegate;
        this.reporter = reporter;
    }

    public ReportingWeightor(TermWeightor delegate) {
        this(new SystemOutReporter(), delegate);
    }

    @Override
    public float weigh(List<QueryTerm> terms) {
        float weight = delegate.weigh(terms);
        reporter.reportTerm(weight, terms);
        return weight;
    }

    @Override
    protected List<QueryTerm> selectWeighted(List<WeightedTermsList> weightedTerms) {
        List<QueryTerm> selected =  delegate.selectWeighted(weightedTerms);
        reporter.reportSelection(weightedTerms, selected);
        return selected;
    }

    public static interface Reporter {

        void reportTerm(float weight, List<QueryTerm> terms);

        void reportSelection(List<WeightedTermsList> terms, List<QueryTerm> selected);

    }

    public static class SystemOutReporter implements Reporter {
        @Override
        public void reportTerm(float weight, List<QueryTerm> terms) {
        }

        @Override
        public void reportSelection(List<TermWeightor.WeightedTermsList> terms, List<QueryTerm> selected) {
            System.out.println("Selected: " + selected + "\n  from: " + terms);
        }
    }
}
