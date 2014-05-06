package uk.co.flax.luwak.presearcher;

import com.google.common.collect.ObjectArrays;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ngram.NGramTokenFilter;
import org.apache.lucene.util.Version;
import uk.co.flax.luwak.termextractor.Extractor;
import uk.co.flax.luwak.termextractor.RegexpNGramTermExtractor;
import uk.co.flax.luwak.util.DuplicateRemovalTokenFilter;

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
 * A Presearcher implementation that matches Wildcard queries by indexing regex
 * terms by their longest static substring, and generates ngrams from InputDocument
 * tokens to match them.
 *
 * This implementation will filter out more wildcard queries than TermFilteredPresearcher,
 * at the expense of longer document build times.  Which one is more performant will depend
 * on the type and number of queries registered in the Monitor, and the size of documents
 * to be monitored.  Profiling is recommended.
 */
public class WildcardNGramPresearcher extends TermFilteredPresearcher {

    /**
     * Create a new WildcardNGramPresearcher using the default QueryTermExtractor
     */
    public WildcardNGramPresearcher(DocumentTokenFilter filter, Extractor... extractors) {
        super(filter, ObjectArrays.concat(extractors, new RegexpNGramTermExtractor()));
    }

    public WildcardNGramPresearcher(Extractor... extractors) {
        this(new DocumentTokenFilter.Default(), extractors);
    }

    @Override
    protected TokenStream filterInputDocumentTokens(String field, TokenStream ts) {
        TokenStream filtered = super.filterInputDocumentTokens(field, ts);
        TokenStream ngrammed = new NGramTokenFilter(Version.LUCENE_50, filtered, 1, Integer.MAX_VALUE);
        return new DuplicateRemovalTokenFilter(ngrammed);
    }
}
