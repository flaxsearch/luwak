package uk.co.flax.luwak.termextractor.treebuilder;

import org.apache.lucene.queries.TermFilter;
import org.apache.lucene.search.TermQuery;
import uk.co.flax.luwak.termextractor.QueryAnalyzer;
import uk.co.flax.luwak.termextractor.QueryTerm;
import uk.co.flax.luwak.termextractor.QueryTreeBuilder;
import uk.co.flax.luwak.termextractor.querytree.QueryTree;
import uk.co.flax.luwak.termextractor.querytree.TermNode;

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

public class TermFilterQueryTreeBuilder extends QueryTreeBuilder<TermFilter> {

    public TermFilterQueryTreeBuilder() {
        super(TermFilter.class);
    }

    @Override
    public QueryTree buildTree(QueryAnalyzer builder, TermFilter query) {
        return new TermNode(new QueryTerm(((TermQuery)query.getQuery()).getTerm()));
    }
}
