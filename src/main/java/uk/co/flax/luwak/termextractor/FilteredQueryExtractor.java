package uk.co.flax.luwak.termextractor;

import java.util.LinkedList;
import java.util.List;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;


public class FilteredQueryExtractor extends Extractor<FilteredQuery> {
    /**
     * The default list of Extractors to use
     */
    public static final List<FilterTermExtractor> DEFAULT_FILTER_EXTRACTORS;

    static {
        DEFAULT_FILTER_EXTRACTORS = new LinkedList<>();
        DEFAULT_FILTER_EXTRACTORS.add(new TermsFilterTermExtractor());
    }

    protected List<FilterTermExtractor> filterTermExtractors = new LinkedList<>();

    public FilteredQueryExtractor() {
        super(FilteredQuery.class);
        this.filterTermExtractors.addAll(DEFAULT_FILTER_EXTRACTORS);
    }

    public FilteredQueryExtractor(List<? extends FilterTermExtractor> filterTermExtractors) {
        this();
        this.filterTermExtractors.addAll(filterTermExtractors);
    }

    @Override
    public void extract(FilteredQuery query, List<QueryTerm> terms, List<Extractor<?>> extractors) {
        Filter filter = query.getFilter();
        for (FilterTermExtractor extractor : this.filterTermExtractors) {
            extractor.extract(filter, terms);
        }
    }
}
