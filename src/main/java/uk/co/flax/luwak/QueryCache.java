package uk.co.flax.luwak;

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

import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;

public interface QueryCache {

    public Entry get(BytesRef hash) throws QueryCacheException;

    public BytesRef put(MonitorQuery query) throws QueryCacheException;

    public static class Entry {

        public final MonitorQuery mq;
        public final Query matchQuery;
        public final Query highlightQuery;

        public Entry(MonitorQuery mq, Query matchQuery, Query highlightQuery) {
            this.mq = mq;
            this.matchQuery = matchQuery;
            this.highlightQuery = highlightQuery;
        }
    }

}
