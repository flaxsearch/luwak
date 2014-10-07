package uk.co.flax.luwak.termextractor;

import org.apache.lucene.search.Query;
import uk.co.flax.luwak.presearcher.DefaultPresearcherComponent;
import uk.co.flax.luwak.presearcher.PresearcherComponent;
import uk.co.flax.luwak.termextractor.querytree.TreeAdvancer;
import uk.co.flax.luwak.termextractor.querytree.QueryTree;
import uk.co.flax.luwak.termextractor.querytree.QueryTreeViewer;
import uk.co.flax.luwak.termextractor.querytree.TreeWeightor;
import uk.co.flax.luwak.termextractor.weights.FieldWeightNorm;
import uk.co.flax.luwak.termextractor.weights.TermTypeNorm;
import uk.co.flax.luwak.termextractor.weights.TermWeightNorm;
import uk.co.flax.luwak.util.ParserUtils;

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

        TreeWeightor weightor = new TreeWeightor(new TermWeightNorm(0.0f, "start"),
                                                 new TermWeightNorm(1, "google"),
                                                 new TermWeightNorm(4, "user", "data"),
                                                 new FieldWeightNorm(0.1f, "wire"),
                                                 new TermTypeNorm(1));
        QueryAnalyzer analyzer = PresearcherComponent.buildQueryAnalyzer(weightor, new DefaultPresearcherComponent());
        TreeAdvancer advancer = new TreeAdvancer.MinWeightTreeAdvancer(analyzer.weightor, 0);

        QueryTree tree = analyzer.buildTree(bq);

        do {
            QueryTreeViewer.view(tree, analyzer.weightor, advancer, System.out);
            System.out.println(analyzer.collectTerms(tree));
        }
        while (analyzer.advancePhase(tree, advancer));

        System.out.flush();
        System.out.println("done");
    }

}
