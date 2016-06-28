package uk.co.flax.luwak.util;

/*
 *   Copyright (c) 2016 Lemur Consulting Ltd.
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

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.*;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestForceNoBulkScoringQuery {

    @Test
    public void testEquality() {

        TermQuery tq1 = new TermQuery(new Term("f", "t"));
        TermQuery tq2 = new TermQuery(new Term("f", "t2"));
        TermQuery tq3 = new TermQuery(new Term("f", "t2"));

        assertThat(new ForceNoBulkScoringQuery(tq1))
                .isEqualTo(new ForceNoBulkScoringQuery(tq1));
        assertThat(new ForceNoBulkScoringQuery(tq1))
                .isNotEqualTo(new ForceNoBulkScoringQuery(tq2));
        assertThat(new ForceNoBulkScoringQuery(tq2))
                .isEqualTo(new ForceNoBulkScoringQuery(tq3));


        assertThat(new ForceNoBulkScoringQuery(tq2).hashCode())
                .isEqualTo(new ForceNoBulkScoringQuery(tq3).hashCode());
        assertThat(new ForceNoBulkScoringQuery(tq1).hashCode())
                .isNotEqualTo(new ForceNoBulkScoringQuery(tq2).hashCode());
    }

    @Test
    public void testRewrite() throws IOException {

        try (Directory dir = new RAMDirectory();
             IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(new StandardAnalyzer()))) {

            Document doc = new Document();
            doc.add(new TextField("field", "term1 term2 term3 term4", Field.Store.NO));
            iw.addDocument(doc);
            iw.commit();

            IndexReader reader = DirectoryReader.open(dir);

            PrefixQuery pq = new PrefixQuery(new Term("field", "term"));
            ForceNoBulkScoringQuery q = new ForceNoBulkScoringQuery(pq);

            assertThat(q.getWrappedQuery()).isEqualTo(pq);

            Query rewritten = q.rewrite(reader);
            assertThat(rewritten).isInstanceOf(ForceNoBulkScoringQuery.class);

            Query inner = ((ForceNoBulkScoringQuery) rewritten).getWrappedQuery();
            assertThat(inner).isNotEqualTo(pq);


        }


    }

}
