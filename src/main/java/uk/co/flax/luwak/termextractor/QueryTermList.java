package uk.co.flax.luwak.termextractor;

import java.util.*;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import uk.co.flax.luwak.termextractor.weights.TermWeightor;

/**
 * Copyright (c) 2014 Lemur Consulting Ltd.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public class QueryTermList implements Iterable<QueryTerm> {

    private final List<QueryTerm> terms;
    private final float weight;

    public QueryTermList(TermWeightor weightor, List<QueryTerm> terms) {
        this.terms = terms;
        this.weight = terms.size() > 0 ? weightor.weigh(terms) : 0;
    }

    public QueryTermList(TermWeightor weightor, QueryTerm... terms) {
        this(weightor, Arrays.asList(terms));
    }

    public static QueryTermList selectBest(List<QueryTermList> termlists) {
        return selectBest(termlists, new Comparator<QueryTermList>() {
            @Override
            public int compare(QueryTermList o1, QueryTermList o2) {
                return Float.compare(o2.weight, o1.weight);
            }
        });
    }

    public static QueryTermList selectBest(List<QueryTermList> termlists, Comparator<QueryTermList> comparator) {
        Collections.sort(termlists, comparator);
        return Iterables.getFirst(termlists, null);
    }

    public static QueryTermList selectBest(QueryTermList... termlists) {
        return selectBest(Arrays.asList(termlists));
    }

    @Override
    public Iterator<QueryTerm> iterator() {
        return terms.iterator();
    }

    public int length() {
        return terms.size();
    }

    @Override
    public String toString() {
        return Joiner.on(" ").join(terms);
    }
}
