package uk.co.flax.luwak.termextractor;

import java.util.List;

import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermsFilter;
import org.junit.Assert;
import org.junit.Test;
import uk.co.flax.luwak.termextractor.treebuilder.TermsFilterQueryTreeBuilder;
import uk.co.flax.luwak.termextractor.querytree.TreeWeightor;

import static org.fest.assertions.api.Assertions.assertThat;

public class TermsFilterQueryTreeBuilderTest {

    private QueryAnalyzer treeBuilder
            = new QueryAnalyzer(TreeWeightor.DEFAULT_WEIGHTOR, new TermsFilterQueryTreeBuilder());

    @Test
    public void testExtractSingleTerm() {
        TermsFilter filter = new TermsFilter(new Term("someField", "123"));

        List<QueryTerm> terms = treeBuilder.collectTerms(treeBuilder.buildTree(filter));

        Assert.assertEquals(1, terms.size());
        Assert.assertEquals("someField", terms.get(0).field);
        Assert.assertEquals("123", terms.get(0).term);
    }

    @Test
    public void testExtractMultipleTerms() {

        TermsFilter filter = new TermsFilter(new Term("field1", "foo"), new Term("field2", "bar"), new Term("field1", "baz"));

        List<QueryTerm> terms = treeBuilder.collectTerms(treeBuilder.buildTree(filter));

        assertThat(terms).hasSize(3);
        assertThat(terms).contains(new QueryTerm("field1", "foo", QueryTerm.Type.EXACT));
        assertThat(terms).contains(new QueryTerm("field2", "bar", QueryTerm.Type.EXACT));
        assertThat(terms).contains(new QueryTerm("field1", "baz", QueryTerm.Type.EXACT));
    }

}
