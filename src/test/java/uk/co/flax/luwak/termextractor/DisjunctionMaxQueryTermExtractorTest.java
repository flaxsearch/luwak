package uk.co.flax.luwak.termextractor;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.Query;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DisjunctionMaxQueryTermExtractorTest {

    private DisjunctionMaxQueryTermExtrator extractor;

    @Before
    public void setUp() {
        extractor = new DisjunctionMaxQueryTermExtrator();
    }

    @Test
    public void shouldTerms() {
        List<QueryTerm> terms = new LinkedList<>();
        Query query1 = new QueryStub(new Term("field1", "foo"));
        Query query2 = new QueryStub(new Term("field2", "bar"));

        DisjunctionMaxQuery disMaxQuery = new DisjunctionMaxQuery(Arrays.asList(query1, query2), 0);

        extractor.extract(disMaxQuery, terms, new LinkedList<Extractor<?>>(Arrays.asList(new ExtractorStub())));
        Assert.assertEquals(2, terms.size());
        Assert.assertEquals("field1", terms.get(0).field);
        Assert.assertEquals("foo", terms.get(0).term);
        Assert.assertEquals("field2", terms.get(1).field);
        Assert.assertEquals("bar", terms.get(1).term);
    }

    private class QueryStub extends Query {

        private Set<Term> terms;

        public QueryStub(Term... terms) {
            this.terms = new HashSet<>(Arrays.asList(terms));
        }

        @Override
        public void extractTerms(Set<Term> terms) {
            terms.addAll(this.terms);
        }

        @Override
        public String toString(String field) {
            return "";
        }
    }

    private class ExtractorStub extends Extractor<QueryStub> {

        public ExtractorStub() {
            super(QueryStub.class);
        }

        @Override
        public void extract(QueryStub query, List<QueryTerm> terms, List<Extractor<?>> extractors) {
            Set<Term> queryTerms = new HashSet<>();
            query.extractTerms(queryTerms);
            for (Term term : queryTerms) {
                terms.add(new QueryTerm(term.field(), term.text(), QueryTerm.Type.EXACT));
            }
        }
    }
}
