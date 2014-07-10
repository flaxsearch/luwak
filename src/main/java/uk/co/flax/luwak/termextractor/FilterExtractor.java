package uk.co.flax.luwak.termextractor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.lucene.search.Filter;

/**
 * Interface for extracting terms from a filter.
 *
 * Subclasses should pass in their own types as a parameter to super().
 */

public abstract class FilterExtractor<T extends Filter> {

    public final Class<T> cls;

    protected FilterExtractor(Class<T> cls) {
        this.cls = cls;
    }

    /**
     * Extract terms from this filter, adding them to a list of terms
     *
     * @param filter the Filter to extract terms from
     * @param terms the List to add the extracted terms to
     */
    public abstract void extract(T filter, List<QueryTerm> terms, Collection<FilterExtractor<?>> filterExtractors);

    @SuppressWarnings("unchecked")
    public static List<QueryTerm> extractTerms(Filter filter, Collection<FilterExtractor<? extends Filter>> filterExtractors) {
        List<QueryTerm> subfilterTerms = new ArrayList<>();
        for (FilterExtractor extractor : filterExtractors) {
            if (extractor.cls.isAssignableFrom(filter.getClass())) {
                extractor.extract(filter, subfilterTerms, filterExtractors);
                break;
            }
        }
        return subfilterTerms;
    }
}
