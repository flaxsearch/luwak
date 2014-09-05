package uk.co.flax.luwak.termextractor;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

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
public class BooleanQueryUtils {

    public static TermQuery newTermQuery(String field, String text) {
        return new TermQuery(new Term(field, text));
    }

    public static class BQBuilder {

        private final BooleanQuery bq;

        static BQBuilder newBQ() {
            return new BQBuilder();
        }

        BQBuilder() {
            this.bq = new BooleanQuery();
        }

        BooleanQuery build() {
            return bq;
        }

        public BQBuilder addMustClause(Query subQuery) {
            bq.add(subQuery, BooleanClause.Occur.MUST);
            return this;
        }

        public BQBuilder addShouldClause(Query subQuery) {
            bq.add(subQuery, BooleanClause.Occur.SHOULD);
            return this;
        }

        public BQBuilder addNotClause(Query subQuery) {
            bq.add(subQuery, BooleanClause.Occur.MUST_NOT);
            return this;
        }
    }
}
