package uk.co.flax.luwak.termextractor.treebuilder;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.PhraseQuery;
import uk.co.flax.luwak.termextractor.QueryTreeBuilder;
import uk.co.flax.luwak.termextractor.QueryTerm;
import uk.co.flax.luwak.termextractor.querytree.ConjunctionNode;
import uk.co.flax.luwak.termextractor.QueryAnalyzer;
import uk.co.flax.luwak.termextractor.querytree.QueryTree;
import uk.co.flax.luwak.termextractor.querytree.TermNode;

public class PhraseQueryTreeBuilder extends QueryTreeBuilder<PhraseQuery> {

    public PhraseQueryTreeBuilder() {
        super(PhraseQuery.class);
    }

    @Override
    public QueryTree buildTree(QueryAnalyzer builder, PhraseQuery query) {
        List<QueryTree> children = new ArrayList<>();
        for (Term term : query.getTerms()) {
            children.add(new TermNode(new QueryTerm(term)));
        }
        return ConjunctionNode.build(children);
    }
}
