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
import org.apache.lucene.util.BytesRef;

public abstract class ParsingQueryCache implements QueryCache {

    private final LoadingCache<MonitorQuery, QueryCacheEntry> queries
            = CacheBuilder.newBuilder().build(new CacheLoader<MonitorQuery, QueryCacheEntry>() {
        @Override
        public QueryCacheEntry load(MonitorQuery mq) throws Exception {
            Query highlightQuery = null;
            if (mq.getHighlightQuery() != null && mq.getHighlightQuery().length > 0)
                highlightQuery = parse(mq.getHighlightQuery());
            return new QueryCacheEntry(parse(mq.getQuery()), highlightQuery);
        }
    });

    protected abstract Query parse(BytesRef query) throws Exception;

    @Override
    public QueryCacheEntry get(MonitorQuery mq) throws QueryCacheException {
        try {
            if (mq.getQuery() == null || mq.getQuery().length == 0)
                return null;
            return queries.get(mq);
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            throw new QueryCacheException(t);
        }
    }
}
