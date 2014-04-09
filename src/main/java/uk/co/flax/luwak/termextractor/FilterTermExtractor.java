package uk.co.flax.luwak.termextractor;

import java.util.List;
import org.apache.lucene.search.Filter;

/**
 * Interface for extracting terms from a filter.
 *
 * Subclasses should pass in their own types as a parameter to super().
 */
public interface FilterTermExtractor {
    /**
     * Extract terms from this filter, adding them to a list of terms
     *
     * @param filter the Filter to extract terms from
     * @param terms the List to add the extracted terms to
     */
    public abstract void extract(Filter filter, List<QueryTerm> terms);
}
