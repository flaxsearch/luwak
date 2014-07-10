package uk.co.flax.luwak.matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import uk.co.flax.luwak.CandidateMatcher;
import uk.co.flax.luwak.InputDocument;
import uk.co.flax.luwak.MatcherFactory;
import uk.co.flax.luwak.QueryMatch;

import org.apache.lucene.search.Query;
import uk.co.flax.luwak.MatchError;

public class ConcurrentMatcher<T extends QueryMatch> extends CandidateMatcher<T> {

    private final List<CandidateMatcher<T>> matchers;

    private Map<String, T> matches;

    public ConcurrentMatcher(InputDocument doc, MatcherFactory<CandidateMatcher<T>> factory, int threads) {
        super(doc);
        matchers = new ArrayList<>();
        for (int i = 0; i < threads; ++i) {
            matchers.add(factory.createMatcher(doc));
        }
    }

    private CandidateMatcher<T> chooseMatcher() {
        return matchers.get(ThreadLocalRandom.current().nextInt(0, matchers.size()));
    }

    private void collectMatches() {
        if (matches == null) {
            matches = new HashMap<>();
            for (CandidateMatcher<T> matcher : matchers) {
                for (T match: matcher) {
                    matches.put(match.getQueryId(), match);
                }
            }
        }
    }

    @Override
    public void matchQuery(String queryId, Query matchQuery, Query highlightQuery) {
        CandidateMatcher<T> matcher = chooseMatcher();
        synchronized (matcher) {
            try {
                matcher.matchQuery(queryId, matchQuery, highlightQuery);
            } catch (IOException ex) {
                matcher.getErrors().add(new MatchError(queryId, ex));
            }
        }
    }

    @Override
    public boolean matches(String queryId) {
        collectMatches();
        return matches.containsKey(queryId);
    }

    @Override
    public int getMatchCount() {
        collectMatches();
        return matches.size();
    }

    @Override
    public Iterator<T> iterator() {
        collectMatches();
        return matches.values().iterator();
    }
}
