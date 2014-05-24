package uk.co.flax.luwak;/*
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

import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.lucene.search.Query;

import java.util.concurrent.ExecutionException;

public class ParsingQueryCache implements QueryCache {

    private final MonitorQueryParser parser;

    private final LoadingCache<String, Query> queries = CacheBuilder.newBuilder().build(new CacheLoader<String, Query>() {
        @Override
        public Query load(String query) throws Exception {
            return parser.parse(query);
        }
    });

    public ParsingQueryCache(MonitorQueryParser parser) {
        this.parser = parser;
    }

    @Override
    public Query get(String query) throws QueryCacheException {
        try {
            if (Strings.isNullOrEmpty(query))
                return null;
            return queries.get(query);
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            throw new QueryCacheException(t);
        }
    }
}
