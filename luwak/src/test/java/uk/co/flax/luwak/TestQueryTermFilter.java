package uk.co.flax.luwak;

/*
 *   Copyright (c) 2015 Lemur Consulting Ltd.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestQueryTermFilter {

    private static final String FIELD = "f";

    private static List<Indexable> indexable(String id, String query) {
        QueryCacheEntry e = new QueryCacheEntry(
                new BytesRef(id.getBytes(StandardCharsets.UTF_8)),
                new TermQuery(new Term(FIELD, query)),
                new HashMap<String, String>()
        );
        Document doc = new Document();
        doc.add(new StringField(FIELD, query, Field.Store.NO));
        return ImmutableList.of(
                new Indexable(id, e, doc)
        );
    }

    @Test
    public void testFiltersAreRemoved() throws IOException {

        QueryIndex qi = new QueryIndex();
        qi.commit(indexable("1", "term"));
        assertThat(qi.termFilters).hasSize(1);
        qi.commit(indexable("2", "term2"));
        assertThat(qi.termFilters).hasSize(1);

        QueryTermFilter tf = Iterables.getFirst(qi.termFilters.values(), null);
        assertThat(tf).isNotNull();
        assertThat(tf.getTerms(FIELD).size()).isEqualTo(2);
    }

}
