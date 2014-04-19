package uk.co.flax.luwak.termextractor;

import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.Query;

public class DisjunctionMaxQueryTermExtrator extends Extractor<DisjunctionMaxQuery> {

    public DisjunctionMaxQueryTermExtrator() {
        super(DisjunctionMaxQuery.class);
    }

    @Override
    public void extract(DisjunctionMaxQuery query, List<QueryTerm> terms, List<Extractor<?>> extractors) {
        ArrayList<Query> disjuncts = query.getDisjuncts();
        for (Query subQuery : disjuncts) {
            extractTerms(subQuery, terms, extractors);
        }
    }

}
