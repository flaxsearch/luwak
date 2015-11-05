package uk.co.flax.luwak;

import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.Query;

/**
 * Copyright (c) 2013 Lemur Consulting Ltd.
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

/**
 * Abstract base class of all Presearcher implementations
 *
 * A Presearcher is used by the Monitor to reduce the number of queries actually
 * run against an InputDocument.  It defines how queries are stored in the monitor's
 * internal index, and how an InputDocument is converted to a query against that
 * index.
 *
 * See {@link uk.co.flax.luwak.presearcher.MatchAllPresearcher}
 * See {@link uk.co.flax.luwak.presearcher.TermFilteredPresearcher}
 * See {@link uk.co.flax.luwak.presearcher.WildcardNGramPresearcherComponent}
 */
public abstract class Presearcher {

    /**
     * Build a query for a Monitor's queryindex from a LeafReader over a {@link DocumentBatch}.
     * @param reader a {@link LeafReader} over the input {@link DocumentBatch} index
     * @param queryTermFilter a {@link QueryTermFilter} for the queryindex
     * @return a Query to run over a Monitor's queryindex
     */
    public abstract Query buildQuery(LeafReader reader, QueryTermFilter queryTermFilter);

    /**
     * Build a lucene Document to index the query in a Monitor's queryindex
     * @param query the Query to index
     * @param metadata a Map of arbitrary query metadata
     * @return a lucene Document to add to the queryindex
     */
    public abstract Document indexQuery(Query query, Map<String, String> metadata);

}
