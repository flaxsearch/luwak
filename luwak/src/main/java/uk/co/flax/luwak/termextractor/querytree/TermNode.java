package uk.co.flax.luwak.termextractor.querytree;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.ToDoubleFunction;

import org.apache.lucene.index.Term;
import uk.co.flax.luwak.termextractor.QueryTerm;
import uk.co.flax.luwak.termextractor.weights.TermWeightor;

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

public class TermNode extends QueryTree {

    private final QueryTerm term;
    private final double weight;

    public TermNode(QueryTerm term, double weight) {
        this.term = term;
        this.weight = weight;
    }

    public TermNode(QueryTerm term, TermWeightor weightor) {
        this(term, weightor.weigh(term));
    }

    public TermNode(Term term, TermWeightor weightor) {
        this(new QueryTerm(term), weightor);
    }

    @Override
    public double weight() {
        return weight;
    }

    @Override
    public void collectTerms(Set<QueryTerm> termsList) {
        termsList.add(term);
    }

    @Override
    public boolean advancePhase(float minWeight) {
        return false;
    }

    @Override
    public void visit(QueryTreeVisitor visitor, int depth) {
        visitor.visit(this, depth);
    }

    @Override
    public boolean isAny() {
        return term.type == QueryTerm.Type.ANY;
    }

    @Override
    public String toString() {
        return "Node [" + term.toString() + "]^" + weight;
    }
}
