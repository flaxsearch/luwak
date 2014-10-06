package uk.co.flax.luwak.termextractor.querytree;

import java.io.PrintStream;

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

public class QueryTreeViewer implements QueryTreeVisitor {

    private final PrintStream out;

    private final TreeWeightor weightor;

    private final Advancer advancer;

    public QueryTreeViewer(TreeWeightor weightor, Advancer advancer, PrintStream out) {
        this.out = out;
        this.weightor = weightor;
        this.advancer = advancer;
    }

    public static void view(QueryTree tree, TreeWeightor weightor, Advancer advancer, final PrintStream out) {
        tree.visit(new QueryTreeViewer(weightor, advancer, out));
    }

    public static void view(QueryTree tree, TreeWeightor weightor, final PrintStream out) {
        view(tree, weightor, Advancer.DEFAULT, out);
    }

    @Override
    public void visit(QueryTree tree, int depth) {
        for (int i = 0; i < depth; i++) {
            out.print("\t");
        }
        out.println(tree.toString() + " <-- [" + tree.toString(weightor) + "]"
                + (tree.isAdvanceable(advancer) ? " ADVANCEABLE" : ""));
    }
}
