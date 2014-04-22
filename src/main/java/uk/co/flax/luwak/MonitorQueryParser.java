package uk.co.flax.luwak;

import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
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

public abstract class MonitorQueryParser {

    protected abstract Query parse(String queryString) throws MonitorQueryParserException;

    protected abstract String hash(String query, String hl);

    public MonitorQuery createQuery(String id, String query, String hl, String hash) throws MonitorQueryParserException {
        return new MonitorQuery(id, parse(query), parse(hl), hash(query, hl));
    }

    public abstract static class SHAHashing extends MonitorQueryParser {

        private static final MessageDigest digest;
        static {
            try {
                digest = MessageDigest.getInstance("SHA-1");
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException("Can't initialise SHAHashing parser", e);
            }
        }

        @Override
        public String hash(String query, String hl) {
            return new String(digest.digest((query + hl).getBytes()));
        }

    }

    public static class LuceneMonitorQueryParser extends SHAHashing {

        private final QueryParser parser;

        public LuceneMonitorQueryParser(QueryParser parser) {
            this.parser = parser;
        }

        @Override
        protected Query parse(String queryString) throws MonitorQueryParserException {
            try {
                return parser.parse(queryString);
            } catch (ParseException e) {
                throw new MonitorQueryParserException(e);
            }
        }
    }
}
