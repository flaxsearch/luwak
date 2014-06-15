package uk.co.flax.luwak.termextractor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Filter;
import static uk.co.flax.luwak.termextractor.Extractor.extractTerms;
import uk.co.flax.luwak.termextractor.weights.CompoundRuleWeightor;
import uk.co.flax.luwak.termextractor.weights.TermWeightor;


public class ConstantScoreQueryTermExtractor extends Extractor<ConstantScoreQuery> {

    /**
     * The default list of Extractors to use
     */
    public static final List<FilterTermExtractor<? extends Filter>> DEFAULT_FILTER_EXTRACTORS = ImmutableList.of(
            new TermsFilterTermExtractor(),
            new TermFilterTermExtractor(),
            new GenericFilterTermExtractor()
    );

    private List<FilterTermExtractor<? extends Filter>> filterTermExtractors
            = new LinkedList<>(DEFAULT_FILTER_EXTRACTORS);

    private final TermWeightor weightor;

    public ConstantScoreQueryTermExtractor() {
        this(CompoundRuleWeightor.DEFAULT_WEIGHTOR);
    }

    public ConstantScoreQueryTermExtractor(TermWeightor weightor) {
        this(weightor, null);
    }

    public ConstantScoreQueryTermExtractor(TermWeightor weightor,
            List<FilterTermExtractor<? extends Filter>> filterTermExtractors) {
        super(ConstantScoreQuery.class);
        this.weightor = weightor;
        if (filterTermExtractors != null) {
            this.filterTermExtractors.addAll(0, filterTermExtractors);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void extract(ConstantScoreQuery query, List<QueryTerm> terms, List<Extractor<?>> extractors) {
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
