package uk.co.flax.luwak.termextractor;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Iterables;
import org.apache.lucene.search.FilteredQuery;
import uk.co.flax.luwak.termextractor.weights.CompoundRuleWeightor;
import uk.co.flax.luwak.termextractor.weights.TermWeightor;


public class FilteredQueryExtractor extends Extractor<FilteredQuery> {

    private final FilterTermExtractor fte;
    private final TermWeightor weightor;

    public FilteredQueryExtractor() {
        this(CompoundRuleWeightor.DEFAULT_WEIGHTOR);
    }

    public FilteredQueryExtractor(TermWeightor weightor) {
        this(weightor, new FilterTermExtractor(weightor));
    }

    public FilteredQueryExtractor(FilterTermExtractor fte) {
        this(CompoundRuleWeightor.DEFAULT_WEIGHTOR, fte);
    }

    public FilteredQueryExtractor(TermWeightor weightor, FilterTermExtractor fte) {
        super(FilteredQuery.class);
        this.weightor = weightor;
        this.fte = fte;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void extract(FilteredQuery query, List<QueryTerm> terms, List<Extractor<?>> extractors) {

        List<QueryTerm> subqueryTerms = new ArrayList<>();
        extractTerms(query.getQuery(), subqueryTerms, extractors);
        QueryTermList queryTerms = new QueryTermList(weightor, subqueryTerms);

        QueryTermList filterTerms = new QueryTermList(weightor, fte.extract(query.getFilter()));

        Iterables.addAll(terms, QueryTermList.selectBest(queryTerms, filterTerms));

    }

}
