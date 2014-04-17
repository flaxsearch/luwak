/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uk.co.flax.luwak.termextractor;

import java.util.List;
import org.apache.lucene.search.WildcardQuery;


public class WildcardQueryTermExtractor extends Extractor<WildcardQuery> {

    public WildcardQueryTermExtractor() {
        super(WildcardQuery.class);
    }

    @Override
    public void extract(WildcardQuery query, List<QueryTerm> terms, List<Extractor<?>> extractors) {
        terms.add(new QueryTerm(
                query.getField(), query.getTerm().text().replaceAll("\\*|\\?", ""), QueryTerm.Type.WILDCARD));
    }
}
