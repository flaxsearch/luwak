package uk.co.flax.luwak;

import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermsFilter;
import org.apache.lucene.search.Filter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import uk.co.flax.luwak.termextractor.QueryTerm;
import uk.co.flax.luwak.termextractor.TermsFilterTermExtractor;

import java.util.LinkedList;
import java.util.List;

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
        Filter filter = new TermsFilter(new Term("someField", "123"));
        extractor.extract(filter, terms);
        Assert.assertEquals(1, terms.size());
        Assert.assertEquals("someField", terms.get(0).field);
        Assert.assertEquals("123", terms.get(0).term);
    }

    @Test
    public void testExtractMultipleTerms() {

        Filter filter = new TermsFilter(new Term("field1", "foo"), new Term("field2", "bar"), new Term("field1", "baz"));

        List<QueryTerm> terms = new LinkedList<>();
        extractor.extract(filter, terms);

        assertThat(terms).hasSize(3);
        assertThat(terms).contains(new QueryTerm("field1", "foo", QueryTerm.Type.EXACT));
        assertThat(terms).contains(new QueryTerm("field2", "bar", QueryTerm.Type.EXACT));
        assertThat(terms).contains(new QueryTerm("field1", "baz", QueryTerm.Type.EXACT));
    }

}
