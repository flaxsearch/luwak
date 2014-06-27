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

import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Strings;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import uk.co.flax.luwak.MonitorQuery;
import uk.co.flax.luwak.QueryCache;
import uk.co.flax.luwak.QueryCacheException;

public abstract class ParsingQueryCache implements QueryCache {

    private final MonitorQueryHasher hasher;

    private final Map<BytesRef, Entry> entries = new HashMap<>();

    protected ParsingQueryCache(MonitorQueryHasher hasher) {
        this.hasher = hasher;
    }

    protected ParsingQueryCache() {
        this(new MonitorQueryHasher.MD5Hasher());
    }

    protected abstract Query parse(String queryString, Map<String, String> metadata) throws Exception;

    @Override
    public final BytesRef put(MonitorQuery query) throws QueryCacheException {
        try {
            Query matchQuery = parse(query.getQuery(), query.getMetadata());
            Query highlightQuery = Strings.isNullOrEmpty(query.getHighlightQuery())
                    ? null : parse(query.getHighlightQuery(), query.getMetadata());
            Entry entry = new Entry(query, matchQuery, highlightQuery);
            BytesRef hash = hasher.hash(query);
            entries.put(hash, entry);
            return hash;
        } catch (Exception e) {
            throw new QueryCacheException(e);
        }
    }

    @Override
    public final Entry get(BytesRef hash) throws QueryCacheException {
        return entries.get(hash);
    }
}
