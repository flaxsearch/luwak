package uk.co.flax.luwak.termextractor.querytree;

import java.util.ArrayList;
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

public class DisjunctionNode extends QueryTree {
    private static final float MINIMUM_WEIGHT = -Float.MAX_VALUE;
    private Boolean isAdvanceable;
    private float weight = MINIMUM_WEIGHT;

    private DisjunctionNode(List<QueryTree> children) {
        for (QueryTree child : children) {
            this.addChild(child);
        }
    }

    public static QueryTree build(List<QueryTree> children) {
        if (children.size() == 0)
            throw new IllegalArgumentException("Cannot build DisjunctionNode with no children");
        if (children.size() == 1)
            return children.get(0);
        return new DisjunctionNode(children);
    }

    @Override
    public float weight(TreeWeightor weightor) {
        if (weight == MINIMUM_WEIGHT) {
            weight = weightor.combine(children);
        }
        return weight;
    }


    @Override
    protected void addChild(QueryTree child) {
        super.addChild(child);
        if(isAdvanceable != null)
            throw new IllegalStateException(); //Just making sure about this.. Otherwise we need to clear on every addChild
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
    public boolean isAdvanceable(TreeAdvancer advancer) {
        if (isAdvanceable == null) {
            boolean result = false;
            for (QueryTree child : children) {
                result |= child.isAdvanceable(advancer); //TODO can't this break if true?
            }
            isAdvanceable = result;
        }
        return isAdvanceable;
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
    public String toString(TreeWeightor weightor, TreeAdvancer advancer) {
        StringBuilder sb = new StringBuilder("Disjunction[");
        sb.append(children.size()).append("] ");
        sb.append(weight(weightor)).append(" { ");
        for (QueryTree child : children) {
            sb.append(child.terms(weightor)).append(" ");
        }
        return sb.append("}").toString();
    }

    @Override
    public Set<QueryTerm> terms(TreeWeightor weightor) {
        List<QueryTerm> qterms = new ArrayList<>();
        this.collectTerms(qterms, weightor);
        Set<QueryTerm> terms = new HashSet<>();
        terms.addAll(qterms);
        return terms;
    }

    @Override
    public boolean advancePhase(TreeWeightor weightor, TreeAdvancer advancer) {
        boolean changed = false;
        for (QueryTree child : children) {
            changed |= child.advancePhase(weightor, advancer);
        }
        if (changed) {
            clearCachedValues();
        }
        return changed;
    }

    private void clearCachedValues() {
        isAdvanceable = null;
        weight = -Float.MAX_VALUE;
    }

    @Override
    public boolean hasAdvanceableDescendents(TreeAdvancer advancer) {
        for (QueryTree child : children) {
            if (child.hasAdvanceableDescendents(advancer))
                return true;
        }
        return false;
    }

    @Override
    public void visit(QueryTreeVisitor visitor, int depth) {
        visitor.visit(this, depth);
        for (QueryTree child : children) {
            child.visit(visitor, depth + 1);
        }
    }

}
