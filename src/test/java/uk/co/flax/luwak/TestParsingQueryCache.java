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

import org.apache.lucene.search.Query;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import uk.co.flax.luwak.parsers.LuceneQueryCache;

import static org.fest.assertions.api.Assertions.assertThat;

public class TestParsingQueryCache {

    @Test
    public void testQueryOnlyParsesOnce() throws QueryCacheException {
        LuceneQueryCache cache = new LuceneQueryCache("f");
        Query query1 = cache.get("test");
        Query query2 = cache.get("test");

        assertThat(query1).isSameAs(query2);
    }

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Test
    public void testExceptionsAreCorrectlyWrapped() throws QueryCacheException {
        LuceneQueryCache cache = new LuceneQueryCache("f");

        expected.expect(QueryCacheException.class);
        expected.expectMessage("Was expecting one of");
        cache.get("test (+hello");
    }

}
