package uk.co.flax.luwak.termextractor;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;


public class FilteredQueryExtractor extends Extractor<FilteredQuery> {
    /**
     * The default list of Extractors to use
     */
    public static final List<FilterTermExtractor> filterTermExtractors = new LinkedList<FilterTermExtractor>(
        Arrays.asList(
            new TermsFilterTermExtractor()
        )
    );

    public FilteredQueryExtractor() {
        super(FilteredQuery.class);
    }

    public FilteredQueryExtractor(List<? extends FilterTermExtractor> filterTermExtractors) {
        super(FilteredQuery.class);
        FilteredQueryExtractor.filterTermExtractors.addAll(filterTermExtractors);
    }

    @Override
    public void extract(FilteredQuery query, List<QueryTerm> terms, List<Extractor<?>> extractors) {
        Filter filter = query.getFilter();
        for (FilterTermExtractor extractor : this.filterTermExtractors) {
            extractor.extract(filter, terms);
        }
    }
}
