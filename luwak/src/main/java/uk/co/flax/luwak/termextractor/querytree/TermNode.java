package uk.co.flax.luwak.termextractor.querytree;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

public class TermNode extends QueryTree {

    protected final QueryTerm term;

    public TermNode(QueryTerm term) {
        this.term = term;
    }

    @Override
    public void addChild(QueryTree child) {
        throw new UnsupportedOperationException("Cannot add child to a TermNode");
    }

    @Override
    public float weight(TreeWeightor weightor) {
        return weightor.weigh(term);
    }

    @Override
    public void collectTerms(List<QueryTerm> termsList, TreeWeightor weightor) {
        termsList.add(term);
    }

    @Override
    public boolean advancePhase(TreeWeightor weightor, TreeAdvancer advancer) {
        return false;
    }

    @Override
    public void visit(QueryTreeVisitor visitor, int depth) {
        visitor.visit(this, depth);
    }

    @Override
    public boolean isAdvanceable(TreeAdvancer advancer) {
        return false;
    }

    @Override
    public boolean hasAdvanceableDescendents(TreeAdvancer advancer) {
        return false;
    }

    @Override
    public boolean isAny() {
        return term.type == QueryTerm.Type.ANY;
    }

    @Override
    public String toString(TreeWeightor weightor, TreeAdvancer advancer) {
        return this.toString() + " " + this.weight(weightor);
    }

    @Override
    public Set<QueryTerm> terms(TreeWeightor weightor) {
        Set<QueryTerm> ts = new HashSet<>();
        ts.add(term);
        return ts;
    }

    @Override
    public String toString() {
        return "Node [" + term.toString() + "]";
    }
}
