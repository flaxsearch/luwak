package uk.co.flax.luwak.querycache;/*
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

import com.google.common.base.Strings;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import uk.co.flax.luwak.Constants;
import uk.co.flax.luwak.QueryCache;

public class LuceneQueryCache extends QueryCache {

    private final QueryParser parser;

    public LuceneQueryCache(String defaultField, Analyzer analyzer) {
        super();
        this.parser = new QueryParser(Constants.VERSION, defaultField, analyzer);
    }

    public LuceneQueryCache(String defaultField) {
        this(defaultField,  new StandardAnalyzer(Constants.VERSION));
    }

    @Override
    protected Query getQuery(String query, Map<String, String> metadata) throws Exception {
        if (Strings.isNullOrEmpty(query))
            return null;
        return parser.parse(query);
    }
}
