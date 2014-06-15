package uk.co.flax.luwak.termextractor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import uk.co.flax.luwak.termextractor.weights.CompoundRuleWeightor;
import uk.co.flax.luwak.termextractor.weights.TermWeightor;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;


public class FilteredQueryExtractor extends Extractor<FilteredQuery> {
    /**
     * The default list of Extractors to use
     */
    public static final List<FilterTermExtractor<? extends Filter>> DEFAULT_FILTER_EXTRACTORS = ImmutableList.of(
            new TermsFilterTermExtractor(),
            new TermFilterTermExtractor(),
            new GenericFilterTermExtractor()
    );

    protected List<FilterTermExtractor<? extends Filter>> filterTermExtractors = new LinkedList<>(DEFAULT_FILTER_EXTRACTORS);

    private final TermWeightor weightor;

    public FilteredQueryExtractor() {
        this(CompoundRuleWeightor.DEFAULT_WEIGHTOR);
    }

    public FilteredQueryExtractor(TermWeightor weightor) {
        this(weightor, null);
    }

    public FilteredQueryExtractor(TermWeightor weightor, List<FilterTermExtractor<? extends Filter>> filterTermExtractors) {
        super(FilteredQuery.class);
        this.weightor = weightor;
        if (filterTermExtractors != null) {
            this.filterTermExtractors.addAll(0, filterTermExtractors);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void extract(FilteredQuery query, List<QueryTerm> terms, List<Extractor<?>> extractors) {

        List<QueryTerm> subqueryTerms = new ArrayList<>();
        if (query.getQuery() != null) {
            extractTerms(query.getQuery(), subqueryTerms, extractors);
        }
        QueryTermList queryTerms = new QueryTermList(weightor, subqueryTerms);

        List<QueryTerm> subfilterTerms = new ArrayList<>();
        Filter filter = query.getFilter();
        if (filter != null) {
            for (FilterTermExtractor extractor : this.filterTermExtractors) {
                if (extractor.cls.isAssignableFrom(filter.getClass())) {
                    extractor.extract(filter, subfilterTerms);
                    break;
                }
            }
        }
        QueryTermList filterTerms = new QueryTermList(weightor, subfilterTerms);

        if (queryTerms.length() == 0) {
            Iterables.addAll(terms, filterTerms);
        } else {
            Iterables.addAll(terms, QueryTermList.selectBest(queryTerms, filterTerms));
        }
    }
}
