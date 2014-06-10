package uk.co.flax.luwak.termextractor;

import org.apache.lucene.search.Filter;

import java.util.List;

/**
 * Interface for extracting terms from a filter.
 *
 * Subclasses should pass in their own types as a parameter to super().
 */

public abstract class FilterTermExtractor<T extends Filter> {

    public final Class<T> cls;

    protected FilterTermExtractor(Class<T> cls) {
        this.cls = cls;
    }

    /**
     * Extract terms from this filter, adding them to a list of terms
     *
     * @param filter the Filter to extract terms from
     * @param terms the List to add the extracted terms to
     */
    public abstract void extract(T filter, List<QueryTerm> terms);
}
