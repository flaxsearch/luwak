package uk.co.flax.luwak.matchers;

import uk.co.flax.luwak.CandidateMatcher;
import uk.co.flax.luwak.InputDocument;
import uk.co.flax.luwak.MatcherFactory;
import uk.co.flax.luwak.QueryMatch;

public class ConcurrentMatcherFactory<T extends QueryMatch> implements MatcherFactory<CandidateMatcher<T>> {

    private final MatcherFactory matcherFactory;
    private final int threads;

    public ConcurrentMatcherFactory(MatcherFactory matcherFactory, int threads) {
        this.matcherFactory = matcherFactory;
        this.threads = threads;
    }

    @Override
    public CandidateMatcher<T> createMatcher(InputDocument doc) {
        return new ConcurrentMatcher(doc, matcherFactory, threads);
    }

}
