package uk.co.flax.luwak;

import java.util.LinkedList;
import java.util.List;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermsFilter;
import org.apache.lucene.search.Filter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import uk.co.flax.luwak.termextractor.QueryTerm;
import uk.co.flax.luwak.termextractor.TermsFilterTermExtractor;

public class TermsFilterTermExtractorTest {

    private TermsFilterTermExtractor extractor;

    @Before
    public void setUp() {
        extractor = new TermsFilterTermExtractor();
    }

    @Test
    public void testExtract() {
        List<QueryTerm> terms = new LinkedList<>();
        Filter filter = new TermsFilter(new Term("someField", "123"));
        extractor.extract(filter, terms);
        Assert.assertEquals(1, terms.size());
        Assert.assertEquals("someField", terms.get(0).field);
        Assert.assertEquals("123", terms.get(0).term);
    }

}
