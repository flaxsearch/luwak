/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package uk.co.flax.luwak.termextractor;
import java.util.LinkedList;
import java.util.List;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.WildcardQuery;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class WildcardQueryTermExtractorTest {

    private WildcardQueryTermExtractor extractor;

    @Before
    public void setUp() {
        extractor = new WildcardQueryTermExtractor();
    }

    @Test
    public void shouldExtractTerm() {
        List<QueryTerm> terms = new LinkedList<>();
        WildcardQuery query = new WildcardQuery(new Term("someField", "123*"));
        List<Extractor<?>> extractors = new LinkedList<>();
        extractors.add(new WildcardQueryTermExtractor());
        extractor.extract(query, terms, extractors);
        Assert.assertEquals(1, terms.size());
        Assert.assertEquals("someField", terms.get(0).field);
        Assert.assertEquals("123", terms.get(0).term);
    }
}
