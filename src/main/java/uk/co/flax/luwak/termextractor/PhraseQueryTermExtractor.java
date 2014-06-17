package com.bloomberg.baleene.subscription.luwak;

import com.google.common.collect.Iterables;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.PhraseQuery;
import uk.co.flax.luwak.termextractor.Extractor;
import uk.co.flax.luwak.termextractor.QueryTerm;
import uk.co.flax.luwak.termextractor.QueryTermList;
import uk.co.flax.luwak.termextractor.weights.TermWeightor;

public class PhraseQueryTermExtractor extends Extractor<PhraseQuery> {

    private final TermWeightor weightor;

    public PhraseQueryTermExtractor(TermWeightor weightor) {
        super(PhraseQuery.class);
        this.weightor = weightor;
    }

    @Override
    public void extract(PhraseQuery query, List<QueryTerm> terms, List<Extractor<?>> extractors) {
        Term[] phraseQueryTerms = query.getTerms();
        List<QueryTermList> listOfQueryTermList = new ArrayList<>();
        for (Term term : phraseQueryTerms) {
            listOfQueryTermList.add(new QueryTermList(
                    weightor, new QueryTerm(term.field(), term.text(), QueryTerm.Type.EXACT)));
        }
        Iterables.addAll(terms, QueryTermList.selectBest(listOfQueryTermList));
    }
}
