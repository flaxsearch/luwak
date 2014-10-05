package uk.co.flax.luwak.termextractor.treebuilder;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import uk.co.flax.luwak.termextractor.QueryTreeBuilder;
import uk.co.flax.luwak.termextractor.QueryTerm;
import uk.co.flax.luwak.termextractor.querytree.ConjunctionNode;
import uk.co.flax.luwak.termextractor.QueryAnalyzer;
import uk.co.flax.luwak.termextractor.querytree.QueryTree;
import uk.co.flax.luwak.termextractor.querytree.TermNode;

/**
 * Copyright (c) 2013 Lemur Consulting Ltd.
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

/**
 * Builds a {@link ConjunctionNode} from a generic Query using terms extracted by
 * {@link Query#extractTerms(java.util.Set)}
 */
public class GenericQueryTreeBuilder extends QueryTreeBuilder<Query> {

    public GenericQueryTreeBuilder() {
        super(Query.class);
    }

    @Override
    public QueryTree buildTree(QueryAnalyzer builder, Query query) {
        Set<Term> termSet = new HashSet<>();
        try {
            query.extractTerms(termSet);
        }
        catch (UnsupportedOperationException e) {
            throw new UnsupportedOperationException("Couldn't extract terms from query of type " + query.getClass());
        }

        List<QueryTree> children = new ArrayList<>();
        for (Term term : termSet) {
            children.add(new TermNode(builder.weightor, new QueryTerm(term)));
        }
        return ConjunctionNode.build(builder.weightor, children);

    }
}
