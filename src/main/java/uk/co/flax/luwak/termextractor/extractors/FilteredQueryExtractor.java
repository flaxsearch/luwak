package uk.co.flax.luwak.termextractor.extractors;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Iterables;
import org.apache.lucene.search.FilteredQuery;
import uk.co.flax.luwak.termextractor.Extractor;
import uk.co.flax.luwak.termextractor.QueryTerm;
import uk.co.flax.luwak.termextractor.QueryTermList;
import uk.co.flax.luwak.termextractor.weights.CompoundRuleWeightor;
import uk.co.flax.luwak.termextractor.weights.TermWeightor;


public class FilteredQueryExtractor extends Extractor<FilteredQuery> {

    private final TermWeightor weightor;

    public FilteredQueryExtractor() {
        this(CompoundRuleWeightor.DEFAULT_WEIGHTOR);
    }

    public FilteredQueryExtractor(TermWeightor weightor) {
        super(FilteredQuery.class);
        this.weightor = weightor;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void extract(FilteredQuery query, List<QueryTerm> terms, List<Extractor<?>> extractors) {

        List<QueryTerm> subqueryTerms = new ArrayList<>();
        extractTerms(query.getQuery(), subqueryTerms, extractors);
        QueryTermList queryTerms = new QueryTermList(weightor, subqueryTerms);

        List<QueryTerm> subfilterTerms = new ArrayList<>();
        extractTerms(query.getFilter(), subfilterTerms, extractors);
        QueryTermList filterTerms = new QueryTermList(weightor, subfilterTerms);

        Iterables.addAll(terms, QueryTermList.selectBest(queryTerms, filterTerms));

    }

}
