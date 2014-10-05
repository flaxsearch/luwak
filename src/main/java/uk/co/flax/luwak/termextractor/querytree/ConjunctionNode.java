package uk.co.flax.luwak.termextractor.querytree;

import java.util.Arrays;
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

public class ConjunctionNode extends QueryTree {

    private ConjunctionNode(TreeWeightor weightor, List<QueryTree> children) {
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
        return new ConjunctionNode(weightor, children);
    }

    public static QueryTree build(TreeWeightor weightor, QueryTree... children) {
        return build(weightor, Arrays.asList(children));
    }

    @Override
    public void collectTerms(List<QueryTerm> termsList, TreeWeightor weightor) {
        weightor.select(children).collectTerms(termsList, weightor);
    }

    @Override
    public boolean advancePhase(TreeWeightor weightor) {
        if (!isTerminal()) {
            boolean changed = false;
            for (QueryTree child : children) {
                changed |= child.advancePhase(weightor);
            }
            return changed;
        }
        if (children.size() <= 1)
            return false;
        children.remove(weightor.select(children));
        return true;
    }

    @Override
    public String toString() {
        return (isTerminal() ? "Terminal" : "") + "Conjunction[" + children.size() + "]: " + weight;
    }

    @Override
    public void visit(QueryTreeVisitor visitor, int depth) {
        visitor.visit(this, depth);
        for (QueryTree child : children) {
            child.visit(visitor, depth + 1);
        }
    }

    @Override
    public boolean isTerminal() {
        for (QueryTree child : children) {
            if (child.isTerminal())
                return false;
        }
        return children.size() > 1;
    }

}
