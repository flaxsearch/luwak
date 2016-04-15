package org.apache.lucene.spans;
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

import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.search.*;
import org.apache.lucene.search.spans.SpanOffsetReportingQuery;
import org.apache.lucene.search.spans.SpanRewriter;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestSpanRewriter {

    @Test
    public void testTermsQueryWithMultipleFields() {

        TermsQuery tq = new TermsQuery(new Term("field1", "term1"), new Term("field2", "term1"), new Term("field2", "term2"));

        Query q = new SpanRewriter().rewrite(tq);
        assertThat(q).isInstanceOf(BooleanQuery.class);

    }

    @Test
    public void testBoostQuery() {

        Query q = new SpanRewriter().rewrite(new BoostQuery(new TermQuery(new Term("f", "t")), 2.0f));
        assertThat(q).isInstanceOf(SpanOffsetReportingQuery.class);

    }

    @Test
    public void testMultiTermQueryEquals() {

        WildcardQuery wq = new WildcardQuery(new Term("field", "term"));
        Query q1 = new SpanRewriter().rewrite(wq);
        Query q2 = new SpanRewriter().rewrite(wq);

        assertThat(q1).isEqualTo(q2);

    }

}
