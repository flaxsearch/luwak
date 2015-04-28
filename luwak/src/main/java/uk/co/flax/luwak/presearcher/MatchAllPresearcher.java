package uk.co.flax.luwak.presearcher;

/*
 * Copyright (c) 2013 Lemur Consulting Ltd.
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

import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import uk.co.flax.luwak.InputDocument;
import uk.co.flax.luwak.Presearcher;

/**
 * A simple Presearcher implementation that runs all queries in a Monitor against
 * each supplied InputDocument.
 */
public class MatchAllPresearcher extends Presearcher {

    public MatchAllPresearcher() {
        super();
    }

    @Override
    public Query buildQuery(InputDocument doc, PerFieldTokenFilter filter) {
        return new MatchAllDocsQuery();
    }

    @Override
    public Document indexQuery(Query query, Map<String, String> metadata) {
        return new Document();
    }
}
