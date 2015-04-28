package uk.co.flax.luwak.termextractor.querytree;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

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

    private ConjunctionNode(List<QueryTree> children) {
        for (QueryTree child : children) {
            this.addChild(child);
        }
    }

    public static QueryTree build(List<QueryTree> children) {
        if (children.size() == 0)
            throw new IllegalArgumentException("Cannot build ConjunctionNode with no children");
        if (children.size() == 1)
            return children.get(0);
        return new ConjunctionNode(children);
    }

    public static QueryTree build(QueryTree... children) {
        return build(Arrays.asList(children));
    }

    @Override
    public float weight(TreeWeightor weightor) {
        return weightor.select(children).weight(weightor);
    }

    @Override
    public void collectTerms(List<QueryTerm> termsList, TreeWeightor weightor) {
        weightor.select(children).collectTerms(termsList, weightor);
    }

    @Override
    public boolean advancePhase(final TreeWeightor weightor, TreeAdvancer advancer) {
        if (!isAdvanceable(advancer)) {
            PriorityQueue<QueryTree> pq = buildPriorityQueue(weightor);
            while (pq.size() > 0) {
                QueryTree child = pq.pop();
                if (child.advancePhase(weightor, advancer))
                    return true;
            }
            return false;
        }
        if (children.size() <= 1)
            return false;
        children.remove(weightor.select(children));
        return true;
    }

    private PriorityQueue<QueryTree> buildPriorityQueue(final TreeWeightor weightor) {
        PriorityQueue<QueryTree> pq = new PriorityQueue<QueryTree>(children.size()) {
            @Override
            protected boolean lessThan(QueryTree a, QueryTree b) {
                return a.weight(weightor) > b.weight(weightor);
            }
        };
        for (QueryTree child : children) {
            pq.add(child);
        }
        return pq;
    }

    @Override
    public void visit(QueryTreeVisitor visitor, int depth) {
        visitor.visit(this, depth);
        for (QueryTree child : children) {
            child.visit(visitor, depth + 1);
        }
    }

    @Override
    public boolean isAdvanceable(TreeAdvancer advancer) {
        if (hasAdvanceableDescendents(advancer))
            return false;
        int c = children.size();
        for (QueryTree child : children) {
            if (!advancer.canAdvanceOver(child))
                c--;
        }
        return c > 1;
    }

    @Override
    public boolean hasAdvanceableDescendents(TreeAdvancer advancer) {
        for (QueryTree child : children) {
            if (child.isAdvanceable(advancer) || child.hasAdvanceableDescendents(advancer))
                return true;
        }
        return false;
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
    public String toString(TreeWeightor weightor, TreeAdvancer advancer) {
        return "Conjunction[" + children.size() + "] " + weight(weightor)
                + " " + weightor.select(children).terms(weightor)
                + (isAdvanceable(advancer) ? " ADVANCEABLE" : "");
    }

    @Override
    public Set<QueryTerm> terms(TreeWeightor weightor) {
        return weightor.select(children).terms(weightor);
    }

}
