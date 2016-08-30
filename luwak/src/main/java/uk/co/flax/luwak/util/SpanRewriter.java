package uk.co.flax.luwak.util;
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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.PrefixCodedTerms;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.search.*;
import org.apache.lucene.search.spans.*;
import org.apache.lucene.util.BytesRef;

public class SpanRewriter {

    public static final SpanRewriter INSTANCE = new SpanRewriter();

    public Query rewrite(Query in) {
        if (in instanceof SpanOffsetReportingQuery)
            return in;
        if (in instanceof SpanQuery)
            return forceOffsets((SpanQuery)in);
        if (in instanceof TermQuery)
            return rewriteTermQuery((TermQuery)in);
        if (in instanceof BooleanQuery)
            return rewriteBoolean((BooleanQuery) in);
        if (in instanceof MultiTermQuery)
            return rewriteMultiTermQuery((MultiTermQuery)in);
        if (in instanceof DisjunctionMaxQuery)
            return rewriteDisjunctionMaxQuery((DisjunctionMaxQuery) in);
        if (in instanceof TermsQuery)
            return rewriteTermsQuery((TermsQuery) in);
        if (in instanceof BoostQuery)
            return rewrite(((BoostQuery) in).getQuery());   // we don't care about boosts for rewriting purposes
        if (in instanceof PhraseQuery)
            return rewritePhraseQuery((PhraseQuery)in);

        return rewriteUnknown(in);
    }

    protected final SpanQuery forceOffsets(SpanQuery in) {
        return new SpanOffsetReportingQuery(in);
    }

    protected Query rewriteTermQuery(TermQuery tq) {
        return forceOffsets(new SpanTermQuery(tq.getTerm()));
    }

    protected Query rewriteBoolean(BooleanQuery bq) {
        BooleanQuery.Builder newbq = new BooleanQuery.Builder();
        for (BooleanClause clause : bq) {
            BooleanClause.Occur occur = clause.getOccur();
            if (occur == BooleanClause.Occur.FILTER)
                occur = BooleanClause.Occur.MUST;   // rewrite FILTER to MUST to ensure scoring
            newbq.add(rewrite(clause.getQuery()), occur);
        }
        return new ForceNoBulkScoringQuery(newbq.build());
    }

    protected Query rewriteMultiTermQuery(MultiTermQuery mtq) {
        return forceOffsets(new SpanMultiTermQueryWrapper<>(mtq));
    }

    protected Query rewriteDisjunctionMaxQuery(DisjunctionMaxQuery disjunctionMaxQuery) {
        ArrayList<Query> subQueries = new ArrayList<>();
        for (Query subQuery : disjunctionMaxQuery) {
            subQueries.add(rewrite(subQuery));
        }
        return new DisjunctionMaxQuery(subQueries, disjunctionMaxQuery.getTieBreakerMultiplier());
    }

    protected Query rewriteTermsQuery(TermsQuery query) {
        Map<String, List<SpanTermQuery>> spanQueries = new HashMap<>();

        try {
            Field termsField = TermsQuery.class.getDeclaredField("termData");
            termsField.setAccessible(true);
            PrefixCodedTerms terms = (PrefixCodedTerms) termsField.get(query);
            PrefixCodedTerms.TermIterator it = terms.iterator();
            for (int i = 0; i < terms.size(); i++) {
                BytesRef term = BytesRef.deepCopyOf(it.next());
                if (spanQueries.containsKey(it.field()) == false) {
                    spanQueries.put(it.field(), new ArrayList<SpanTermQuery>());
                }
                spanQueries.get(it.field()).add(new SpanTermQuery(new Term(it.field(), term)));
            }
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            for (Map.Entry<String,List<SpanTermQuery>> entry : spanQueries.entrySet()) {
                List<SpanTermQuery> termQueries = entry.getValue();
                builder.add(new SpanOrQuery(termQueries.toArray(new SpanTermQuery[termQueries.size()])),
                        BooleanClause.Occur.SHOULD);
            }
            return builder.build();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }

    }

    /*
     * This method is only able to rewrite standard phrases where each word must follow the previous one
     * with no gaps or overlaps.  This does however cover all common uses (such as "amazing horse").
     */
    protected Query rewritePhraseQuery(PhraseQuery query) {
        Term[] terms = query.getTerms();
        int[] positions = query.getPositions();
        SpanTermQuery[] spanQueries = new SpanTermQuery[positions.length];

        for(int i = 0; i < positions.length; i++) {
            if(positions[i] - positions[0] != i) {
                // positions must increase by 1 each time (i-1 is safe as the if can't be true for i=0)
                throw new IllegalArgumentException("Don't know how to rewrite PhraseQuery with holes or overlaps " +
                        "(position must increase by 1 each time but found term " + terms[i-1] + " at position " +
                        positions[i-1] + " followed by term " + terms[i] + " at position " + positions[i] + ")");
            }

            spanQueries[i] = new SpanTermQuery(terms[i]);
        }

        return new SpanNearQuery(spanQueries, query.getSlop(), true);
    }

    protected Query rewriteUnknown(Query query) {
        throw new IllegalArgumentException("Don't know how to rewrite " + query.getClass());
    }

}
