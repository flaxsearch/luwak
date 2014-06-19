package uk.co.flax.luwak.parsers;/*
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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import uk.co.flax.luwak.Constants;
import uk.co.flax.luwak.ParsingQueryCache;

public class LuceneQueryCache extends ParsingQueryCache {

    private final QueryParser parser;

    public LuceneQueryCache(String defaultField, Analyzer analyzer) {
        this.parser = new QueryParser(Constants.VERSION, defaultField, analyzer);
    }

    public LuceneQueryCache(String defaultField) {
        this(defaultField,  new StandardAnalyzer(Constants.VERSION));
    }

    @Override
    protected Query parse(BytesRef query) throws Exception {
        return parser.parse(query.utf8ToString());
    }
}
