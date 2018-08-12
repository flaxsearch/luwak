package uk.co.flax.luwak.termextractor;

import org.apache.lucene.search.Query;
import uk.co.flax.luwak.termextractor.querytree.QueryTree;
import uk.co.flax.luwak.termextractor.querytree.QueryTreeViewer;
import uk.co.flax.luwak.termextractor.weights.FieldWeightNorm;
import uk.co.flax.luwak.termextractor.weights.TermWeightNorm;
import uk.co.flax.luwak.termextractor.weights.TermWeightor;
import uk.co.flax.luwak.testutils.ParserUtils;

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

public class DumpQueryTree {

    public static void main(String... args) throws Exception {

        Query bq = ParserUtils.parse("+(+(+story:start term +(+story:data +story:user +story:google)) +(+hello +world +howdyedo))");

        TermWeightor weightor = new TermWeightor(new TermWeightNorm(0.0f, "start"),
                                                 new TermWeightNorm(1, "google"),
                                                 new TermWeightNorm(4, "user", "data"),
                                                 new FieldWeightNorm(0.1f, "wire"));
        QueryAnalyzer analyzer = new QueryAnalyzer();

        QueryTree tree = analyzer.buildTree(bq, weightor);

        do {
            QueryTreeViewer.view(tree, System.out);
            System.out.println(analyzer.collectTerms(tree));
        }
        while (tree.advancePhase(0));

        System.out.flush();
        System.out.println("done");
    }

}
