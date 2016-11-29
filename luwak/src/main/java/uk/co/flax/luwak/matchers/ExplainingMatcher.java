package uk.co.flax.luwak.matchers;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import uk.co.flax.luwak.CandidateMatcher;
import uk.co.flax.luwak.DocumentBatch;
import uk.co.flax.luwak.MatcherFactory;

/*
 * Copyright (c) 2014 Lemur Consulting Ltd.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Return {@link Explanation}s for each match
 */
public class ExplainingMatcher extends CandidateMatcher<ExplainingMatch> {

    /** A factory for ExplainingMatchers */
    public static final MatcherFactory<ExplainingMatch> FACTORY = ExplainingMatcher::new;

    /** Create a new ExplainingMatcher for the provided DocumentBatch
     * @param docs batch of documents to match
     */
    public ExplainingMatcher(DocumentBatch docs) {
        super(docs);
    }

    @Override
    public void doMatchQuery(String queryId, Query matchQuery, Map<String, String> metadata) throws IOException {
        int maxDocs = docs.getIndexReader().maxDoc();
        for (int i = 0; i < maxDocs; i++) {
            Explanation explanation = docs.getSearcher().explain(matchQuery, i);
            if (explanation.isMatch())
                addMatch(new ExplainingMatch(queryId, docs.resolveDocId(i), explanation));
        }
    }

    @Override
    public ExplainingMatch resolve(ExplainingMatch match1, ExplainingMatch match2) {
        return match1.getExplanation().getValue() > match2.getExplanation().getValue() ?
                match1 : match2;
    }
}
