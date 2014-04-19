package uk.co.flax.luwak.termextractor;

import java.util.List;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.PhraseQuery;

/**
 * Extract terms from a phrase query
 */
public class PhraseQueryTermExtractor extends Extractor<PhraseQuery> {

    public PhraseQueryTermExtractor() {
        super(PhraseQuery.class);
    }

    @Override
    public void extract(PhraseQuery query, List<QueryTerm> terms, List<Extractor<?>> extractors) {
        Term[] phraseQueryTerms = query.getTerms();
        for (Term term : phraseQueryTerms) {
            terms.add(new QueryTerm(term.field(), term.text(), QueryTerm.Type.EXACT));
        }
    }
}
