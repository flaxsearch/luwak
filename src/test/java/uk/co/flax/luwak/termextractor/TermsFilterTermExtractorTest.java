package uk.co.flax.luwak.termextractor;

import java.util.LinkedList;
import java.util.List;

import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermsFilter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.fest.assertions.api.Assertions.assertThat;

public class TermsFilterTermExtractorTest {

    private TermsFilterTermExtractor extractor;

    @Before
    public void setUp() {
        extractor = new TermsFilterTermExtractor();
    }

    @Test
    public void testExtractSingleTerm() {
        List<QueryTerm> terms = new LinkedList<>();
        TermsFilter filter = new TermsFilter(new Term("someField", "123"));
        extractor.extract(filter, terms);
        Assert.assertEquals(1, terms.size());
        Assert.assertEquals("someField", terms.get(0).field);
        Assert.assertEquals("123", terms.get(0).term);
    }

    @Test
    public void testExtractMultipleTerms() {

        TermsFilter filter = new TermsFilter(new Term("field1", "foo"), new Term("field2", "bar"), new Term("field1", "baz"));

        List<QueryTerm> terms = new LinkedList<>();
        extractor.extract(filter, terms);

        assertThat(terms).hasSize(3);
        assertThat(terms).contains(new QueryTerm("field1", "foo", QueryTerm.Type.EXACT));
        assertThat(terms).contains(new QueryTerm("field2", "bar", QueryTerm.Type.EXACT));
        assertThat(terms).contains(new QueryTerm("field1", "baz", QueryTerm.Type.EXACT));
    }

}
