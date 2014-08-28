package uk.co.flax.luwak.termextractor.extractors;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Iterables;
import org.apache.lucene.search.FilteredQuery;
import uk.co.flax.luwak.termextractor.Extractor;
import uk.co.flax.luwak.termextractor.QueryTerm;
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

        List<List<QueryTerm>> allTerms = new ArrayList<>();

        List<QueryTerm> subqueryTerms = new ArrayList<>();
        extractTerms(query.getQuery(), subqueryTerms, extractors);
        allTerms.add(subqueryTerms);

        List<QueryTerm> subfilterTerms = new ArrayList<>();
        extractTerms(query.getFilter(), subfilterTerms, extractors);
        allTerms.add(subfilterTerms);

        Iterables.addAll(terms, weightor.selectBest(allTerms));

    }

}
