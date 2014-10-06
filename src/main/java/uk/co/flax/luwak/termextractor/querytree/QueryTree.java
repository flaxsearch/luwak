package uk.co.flax.luwak.termextractor.querytree;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

public abstract class QueryTree {

    protected Set<QueryTree> children = new HashSet<>();

    public QueryTree parent = null;

    public final float weight;

    protected QueryTree(float weight) {
        this.weight = weight;
    }

    protected void addChild(QueryTree child) {
        child.parent = this;
        children.add(child);
    }

    public abstract void collectTerms(List<QueryTerm> termsList, TreeWeightor weightor);

    public abstract boolean advancePhase(TreeWeightor weightor, Advancer advancer);

    public abstract void visit(QueryTreeVisitor visitor, int depth);

    public void visit(QueryTreeVisitor visitor) {
        visit(visitor, 0);
    }

    public abstract boolean isAdvanceable(Advancer advancer);

    public abstract boolean isAny();

    public abstract String toString(TreeWeightor weightor);
}
