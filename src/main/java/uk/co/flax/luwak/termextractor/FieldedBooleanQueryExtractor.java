package uk.co.flax.luwak.termextractor;

import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.intervals.FieldedBooleanQuery;

import java.lang.reflect.Field;
import java.util.List;

/**
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

/**
 * Extract terms from a FieldedBooleanQuery
 *
 * See {@link BooleanTermExtractor}
 */
public class FieldedBooleanQueryExtractor extends Extractor<FieldedBooleanQuery> {

    public FieldedBooleanQueryExtractor() {
        super(FieldedBooleanQuery.class);
    }

    @Override
    public void extract(FieldedBooleanQuery query, List<QueryTerm> terms,
                        List<Extractor<?>> extractors) {
        try {
            Field field = query.getClass().getDeclaredField("bq");
            field.setAccessible(true);
            BooleanQuery bq = (BooleanQuery) field.get(query);
            extractTerms(bq, terms, extractors);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
