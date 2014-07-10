package uk.co.flax.luwak.termextractor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.common.collect.Iterables;
import org.apache.lucene.queries.BooleanFilter;
import org.apache.lucene.queries.FilterClause;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Filter;
import uk.co.flax.luwak.termextractor.weights.TermWeightor;

public class BooleanFilterTermExtractor extends FilterExtractor<BooleanFilter> {

    private TermWeightor termWeightor;

    public BooleanFilterTermExtractor(TermWeightor weightor) {
        super(BooleanFilter.class);
        this.termWeightor = weightor;
    }

    @Override
    public void extract(BooleanFilter filter, List<QueryTerm> terms, Collection<FilterExtractor<? extends Filter>> extractors) {

        Analyzer checker = new Analyzer(filter);

        if (checker.isDisjunctionFilter()) {
            for (Filter subfilter : checker.getDisjunctions()) {
                terms.addAll(extractTerms(subfilter, extractors));
            }
        } else if (checker.isConjunctionFilter()) {
            List<QueryTermList> termlists = new ArrayList<>();
            for (Filter subfilter : checker.getConjunctions()) {
                List<QueryTerm> subTerms = extractTerms(subfilter, extractors);
                termlists.add(new QueryTermList(this.termWeightor, subTerms));
            }
            Iterables.addAll(terms, QueryTermList.selectBest(termlists));
        }
    }

    public static class Analyzer {

        List<Filter> disjunctions = new ArrayList<>();
        List<Filter> conjunctions = new ArrayList<>();

        public Analyzer(BooleanFilter filter) {
            for (FilterClause clause : filter.clauses()) {
                if (clause.getOccur() == BooleanClause.Occur.MUST) {
                    conjunctions.add(clause.getFilter());
                }
                if (clause.getOccur() == BooleanClause.Occur.SHOULD) {
                    disjunctions.add(clause.getFilter());
                }
            }
        }

        public boolean isConjunctionFilter() {
            return conjunctions.size() > 0;
        }

        public boolean isDisjunctionFilter() {
            return !isConjunctionFilter() && disjunctions.size() > 0;
        }

        public List<Filter> getDisjunctions() {
            return disjunctions;
        }

        public List<Filter> getConjunctions() {
            return conjunctions;
        }
    }

}
