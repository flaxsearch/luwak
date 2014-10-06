package uk.co.flax.luwak.termextractor.querytree;

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
public class DisjunctionNode extends QueryTree {

    private DisjunctionNode(TreeWeightor weightor, List<QueryTree> children) {
        super(weightor.combine(children));
        for (QueryTree child : children) {
            this.addChild(child);
        }
    }

    public static QueryTree build(TreeWeightor weightor, List<QueryTree> children) {
        if (children.size() == 0)
            throw new IllegalArgumentException("Cannot build ConjunctionNode with no children");
        if (children.size() == 1)
            return children.get(0);
        return new DisjunctionNode(weightor, children);
    }

    @Override
    public void collectTerms(List<QueryTerm> termsList, TreeWeightor weightor) {
        if (isAny()) {
            termsList.add(new QueryTerm("", "DISJUNCTION WITH ANYTOKEN", QueryTerm.Type.ANY));
            return;
        }
        for (QueryTree child : children) {
            child.collectTerms(termsList, weightor);
        }
    }

    @Override
    public boolean isAdvanceable(Advancer advancer) {
        boolean result = false;
        for (QueryTree child : children) {
            result |= child.isAdvanceable(advancer);
        }
        return result;
    }

    @Override
    public boolean isAny() {
        for (QueryTree child : children) {
            if (child.isAny())
                return true;
        }
        return false;
    }

    @Override
    public String toString(TreeWeightor weightor) {
        StringBuilder sb = new StringBuilder(" ");
        for (QueryTree child : children) {
            sb.append(child.toString(weightor)).append(" ");
        }
        return sb.toString();
    }

    @Override
    public boolean advancePhase(TreeWeightor weightor, Advancer advancer) {
        boolean changed = false;
        for (QueryTree child : children) {
            changed |= child.advancePhase(weightor, advancer);
        }
        return changed;
    }

    @Override
    public void visit(QueryTreeVisitor visitor, int depth) {
        visitor.visit(this, depth);
        for (QueryTree child : children) {
            child.visit(visitor, depth + 1);
        }
    }

    @Override
    public String toString() {
        return "Disjunction[" + children.size() + "]: " + weight;
    }
}
