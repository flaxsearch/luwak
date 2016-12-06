package uk.co.flax.luwak.termextractor.treebuilder;
/*
 *   Copyright (c) 2016 Lemur Consulting Ltd.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.PrefixCodedTerms;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.util.BytesRef;
import uk.co.flax.luwak.termextractor.QueryAnalyzer;
import uk.co.flax.luwak.termextractor.QueryTreeBuilder;
import uk.co.flax.luwak.termextractor.querytree.DisjunctionNode;
import uk.co.flax.luwak.termextractor.querytree.QueryTree;
import uk.co.flax.luwak.termextractor.querytree.TermNode;

public class TermsQueryTreeBuilder extends QueryTreeBuilder<TermsQuery> {

    public static final TermsQueryTreeBuilder INSTANCE = new TermsQueryTreeBuilder();

    private TermsQueryTreeBuilder() {
        super(TermsQuery.class);
    }

    @Override
    public QueryTree buildTree(QueryAnalyzer builder, TermsQuery query) {
        PrefixCodedTerms.TermIterator it = query.getTermData().iterator();
        List<QueryTree> terms = new ArrayList<>();
        BytesRef term;
        while ((term = it.next()) != null) {
            terms.add(new TermNode(new Term(it.field(), term)));
        }
        return DisjunctionNode.build(terms);
    }
}
