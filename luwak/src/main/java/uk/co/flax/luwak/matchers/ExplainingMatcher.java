package uk.co.flax.luwak.matchers;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanQuery;
import uk.co.flax.luwak.CandidateMatcher;
import uk.co.flax.luwak.InputDocument;
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

public class ExplainingMatcher extends CandidateMatcher<ExplainingMatch> {

    public static final MatcherFactory<ExplainingMatch> FACTORY = new MatcherFactory<ExplainingMatch>() {
        @Override
        public ExplainingMatcher createMatcher(InputDocument doc) {
            return new ExplainingMatcher(doc);
        }
    };

    public ExplainingMatcher(InputDocument doc) {
        super(doc);
    }

    @Override
    protected ExplainingMatch doMatchQuery(String queryId, Query matchQuery, Map<String,String> metadat) throws IOException {
        Explanation explanation = doc.getSearcher().explain(matchQuery, 0);
        if (!explanation.isMatch())
            return null;
        ExplainingMatch result = new ExplainingMatch(queryId, explanation);
        addMatch(queryId, result);
        return result;
    }

    @Override
    public ExplainingMatch resolve(ExplainingMatch match1, ExplainingMatch match2) {
        return match1.getExplanation().getValue() > match2.getExplanation().getValue() ?
                match1 : match2;
    }
}
