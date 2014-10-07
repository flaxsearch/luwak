package uk.co.flax.luwak.termextractor.treebuilder;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.search.FilteredQuery;
import uk.co.flax.luwak.termextractor.QueryTreeBuilder;
import uk.co.flax.luwak.termextractor.querytree.ConjunctionNode;
import uk.co.flax.luwak.termextractor.QueryAnalyzer;
import uk.co.flax.luwak.termextractor.querytree.QueryTree;


public class FilteredQueryTreeBuilder extends QueryTreeBuilder<FilteredQuery> {

    public FilteredQueryTreeBuilder() {
        super(FilteredQuery.class);
    }

    @Override
    public QueryTree buildTree(QueryAnalyzer builder, FilteredQuery query) {
        List<QueryTree> children = new ArrayList<>();
        children.add(builder.buildTree(query.getQuery()));
        children.add(builder.buildTree(query.getFilter()));
        return ConjunctionNode.build(children);
    }

}
