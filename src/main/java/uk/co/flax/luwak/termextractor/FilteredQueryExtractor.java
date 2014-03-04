package uk.co.flax.luwak.termextractor;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;


public class FilteredQueryExtractor extends Extractor<FilteredQuery> {
    private final String ANYTOKEN = "__ANYTOKEN__";

    private final List<Extractor<?>> extractors = new ArrayList<>();

    /**
     * The default list of Extractors to use
     */
    public static final ImmutableList<? extends FilterTermExtractor> DEFAULT_EXTRACTORS = ImmutableList.of(
            new TermsFilterTermExtractor()
    );


    public FilteredQueryExtractor() {
        super(FilteredQuery.class);
    }

    @Override
    public void extract(FilteredQuery query, List<QueryTerm> terms, List<Extractor<?>> extractors) {
        Filter filter = query.getFilter();
        for (FilterTermExtractor extractor : DEFAULT_EXTRACTORS) {
            extractor.extract(filter, terms);
        }
    }
}
