package uk.co.flax.luwak.termextractor.weights;

import java.util.Collection;
import java.util.Set;

import uk.co.flax.luwak.termextractor.QueryTerm;
import uk.co.flax.luwak.termextractor.querytree.QueryTree;
import uk.co.flax.luwak.termextractor.querytree.TreeWeightor;

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

public class ReportingWeightor extends TreeWeightor {

    private final Reporter reporter;

    public ReportingWeightor(Reporter reporter, TreeWeightor delegate) {
        super(delegate);
        this.reporter = reporter;
    }

    public ReportingWeightor(TreeWeightor delegate) {
        this(new SystemOutReporter(), delegate);
    }

    @Override
    public float weigh(QueryTerm term) {
        float weight = super.weigh(term);
        reporter.reportTerm(weight, term);
        return weight;
    }

    @Override
    public float combine(Collection<QueryTree> children) {
        float weight = super.combine(children);
        reporter.reportCombination(weight, children);
        return weight;
    }

    @Override
    public QueryTree select(Set<QueryTree> children) {
        QueryTree selected = super.select(children);
        reporter.reportSelected(selected, children);
        return selected;
    }

    public static interface Reporter {

        void reportTerm(float weight, QueryTerm term);

        void reportSelected(QueryTree selected, Set<QueryTree> children);

        void reportCombination(float weight, Collection<QueryTree> children);

    }

    public static class SystemOutReporter implements Reporter {

        @Override
        public void reportTerm(float weight, QueryTerm term) {
            System.out.println("Term: " + term + " weight: " + weight);
        }

        @Override
        public void reportSelected(QueryTree selected, Set<QueryTree> children) {
            System.out.println("Selected " + selected + "\n\tfrom " + children);
        }

        @Override
        public void reportCombination(float weight, Collection<QueryTree> children) {
            System.out.println("Derived weight " + weight + " from combination of " + children);
        }


    }
}
