package uk.co.flax.luwak.termextractor.querytree;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.lucene.util.PriorityQueue;
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

public class ConjunctionNode extends QueryTree {

    private static final Comparator<QueryTree> COMPARATOR = Comparator.comparingDouble(QueryTree::weight).reversed();

    private final List<QueryTree> children = new ArrayList<>();

    private ConjunctionNode(List<QueryTree> children) {
        this.children.addAll(children);
        this.children.sort(COMPARATOR);
    }

    public static QueryTree build(List<QueryTree> children) {
        if (children.size() == 0)
            throw new IllegalArgumentException("Cannot build ConjunctionNode with no children");
        if (children.size() == 1)
            return children.get(0);
        List<QueryTree> restrictedChildren = children.stream().filter(c -> c.isAny() == false).collect(Collectors.toList());
        if (restrictedChildren.size() == 0) {
            // all children are ANY nodes, so return the first one
            return children.get(0);
        }
        if (restrictedChildren.size() == 1) {
            return restrictedChildren.get(0);
        }
        return new ConjunctionNode(restrictedChildren);
    }

    public static QueryTree build(QueryTree... children) {
        return build(Arrays.asList(children));
    }

    @Override
    public double weight() {
        return children.get(0).weight();
    }

    @Override
    public void collectTerms(Set<QueryTerm> termsList) {
        children.get(0).collectTerms(termsList);
    }

    @Override
    public boolean advancePhase() {
        if (children.get(0).advancePhase()) {
            this.children.sort(COMPARATOR);
            return true;
        }
        if (children.size() == 1) {
            return false;
        }
        children.remove(0);
        return children.get(0).weight() > 0;
    }

    @Override
    public void visit(QueryTreeVisitor visitor, int depth) {
        visitor.visit(this, depth);
        for (QueryTree child : children) {
            child.visit(visitor, depth + 1);
        }
    }

    @Override
    public boolean isAny() {
        for (QueryTree child : children) {
            if (!child.isAny())
                return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "Conjunction[" + children.size() + "]^" + weight() + " " + children.get(0).toString();
    }

}
