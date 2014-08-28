package uk.co.flax.luwak.termextractor.extractors;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.PhraseQuery;
import uk.co.flax.luwak.termextractor.Extractor;
import uk.co.flax.luwak.termextractor.QueryTerm;
import uk.co.flax.luwak.termextractor.weights.TermWeightor;

public class PhraseQueryTermExtractor extends Extractor<PhraseQuery> {

    private final TermWeightor weightor;

    public PhraseQueryTermExtractor(TermWeightor weightor) {
        super(PhraseQuery.class);
        this.weightor = weightor;
    }

    @Override
    public void extract(PhraseQuery query, List<QueryTerm> terms, List<Extractor<?>> extractors) {
        List<List<QueryTerm>> listOfQueryTermList = new ArrayList<>();
        for (Term term : query.getTerms()) {
            listOfQueryTermList.add(Lists.newArrayList(new QueryTerm(term.field(), term.text(), QueryTerm.Type.EXACT)));
        }
        Iterables.addAll(terms, weightor.selectBest(listOfQueryTermList));
    }
}
