package uk.co.flax.luwak.presearcher;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.KeywordRepeatFilter;
import uk.co.flax.luwak.analysis.DuplicateRemovalTokenFilter;
import uk.co.flax.luwak.analysis.SuffixingNGramTokenFilter;
import uk.co.flax.luwak.termextractor.QueryTerm;
import uk.co.flax.luwak.termextractor.treebuilder.RegexpNGramTermQueryTreeBuilder;

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
public class WildcardNGramPresearcherComponent extends PresearcherComponent {

    /** The default suffix with which to mark ngrams */
    public static final String DEFAULT_NGRAM_SUFFIX = "XX";

    /** The default maximum length of an input token before ANYTOKENS are generated */
    public static final int DEFAULT_MAX_TOKEN_SIZE = 30;

    /** The default token to emit if a term is longer than MAX_TOKEN_SIZE */
    public static final String DEFAULT_WILDCARD_TOKEN = "__WILDCARD__";

    private final String ngramSuffix;

    private final String wildcardToken;

    private final int maxTokenSize;

    /**
     * Create a new WildcardNGramPresearcherComponent
     * @param ngramSuffix the suffix with which to mark ngrams
     * @param maxTokenSize the maximum length of an input token before WILDCARD tokens are generated
     * @param wildcardToken the token to emit if a token is longer than maxTokenSize in length
     */
    public WildcardNGramPresearcherComponent(String ngramSuffix, int maxTokenSize, String wildcardToken) {
        super(new RegexpNGramTermQueryTreeBuilder(ngramSuffix, wildcardToken));
        this.ngramSuffix = ngramSuffix;
        this.maxTokenSize = maxTokenSize;
        this.wildcardToken = wildcardToken;
    }

    /**
     * Create a new WildcardNGramPresearcherComponent using default settings
     */
    public WildcardNGramPresearcherComponent() {
        this(DEFAULT_NGRAM_SUFFIX, DEFAULT_MAX_TOKEN_SIZE, DEFAULT_WILDCARD_TOKEN);
    }

    @Override
    public TokenStream filterDocumentTokens(TokenStream ts) {
        TokenStream duped = new KeywordRepeatFilter(ts);
        TokenStream ngrammed
                = new SuffixingNGramTokenFilter(duped, ngramSuffix, wildcardToken, maxTokenSize);
        return new DuplicateRemovalTokenFilter(ngrammed);
    }

    @Override
    public String extraToken(QueryTerm term) {
        if (term.type == QueryTerm.Type.CUSTOM && wildcardToken.equals(term.payload))
            return wildcardToken;
        return null;
    }
}
