package uk.co.flax.luwak.presearcher;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.collect.ObjectArrays;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.KeywordRepeatFilter;
import uk.co.flax.luwak.analysis.DuplicateRemovalTokenFilter;
import uk.co.flax.luwak.analysis.SuffixingNGramTokenFilter;
import uk.co.flax.luwak.termextractor.QueryTreeBuilder;
import uk.co.flax.luwak.termextractor.treebuilder.RegexpNGramTermQueryTreeBuilder;
import uk.co.flax.luwak.termextractor.querytree.TreeWeightor;

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

    /** The default suffix with which to mark ngrams */
    public static final String DEFAULT_NGRAM_SUFFIX = "XX";

    /** The default maximum length of an input token before ANYTOKENS are generated */
    public static final int DEFAULT_MAX_TOKEN_SIZE = 30;

    private final String ngramSuffix;

    private final int maxTokenSize;

    /**
     * Create a new WildcardNGramPresearcher using the default QueryTermExtractor
     */
    protected WildcardNGramPresearcher(TreeWeightor weightor, String ngramSuffix, int maxTokenSize,
                                       QueryTreeBuilder<?>... queryTreeBuilders) {
        super(weightor, ObjectArrays.concat(queryTreeBuilders, new RegexpNGramTermQueryTreeBuilder(ngramSuffix)));
        this.ngramSuffix = ngramSuffix;
        this.maxTokenSize = maxTokenSize;
    }

    @Override
    protected TokenStream filterInputDocumentTokens(String field, TokenStream ts) throws IOException {
        TokenStream duped = new KeywordRepeatFilter(ts);
        TokenStream ngrammed
                = new SuffixingNGramTokenFilter(duped, ngramSuffix, extractor.getAnyToken(), maxTokenSize);
        return new DuplicateRemovalTokenFilter(ngrammed);
    }

    public static final WildcardNGramPresearcher DEFAULT = builder().build();

    /**
     * @return an object to build a new WildcardNGramPresearcher
     */
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private TreeWeightor weightor = TreeWeightor.DEFAULT_WEIGHTOR;
        private List<QueryTreeBuilder<?>> queryTreeBuilders = Lists.newArrayList();
        private String ngramSuffix = DEFAULT_NGRAM_SUFFIX;
        private int maxTokenSize = DEFAULT_MAX_TOKEN_SIZE;

        /**
         * Use this TermWeightor
         * @param weightor the {@link uk.co.flax.luwak.termextractor.weights.TermWeightor}
         * @return the Builder object
         */
        public Builder withWeightor(TreeWeightor weightor) {
            this.weightor = weightor;
            return this;
        }

        /**
         * Use this Extractor
         * @param queryTreeBuilder the Extractor
         * @return the Builder object
         */
        public Builder withExtractor(QueryTreeBuilder<?> queryTreeBuilder) {
            this.queryTreeBuilders.add(queryTreeBuilder);
            return this;
        }

        /**
         * Any tokens larger than this will not be ngrammed, but instead an ANYTOKEN emitted
         * @param size the maximum token size
         * @return the Builder object
         */
        public Builder withMaxTokenSize(int size) {
            this.maxTokenSize = size;
            return this;
        }

        /**
         * Use this suffix to distinguish ngrams from their parent tokens
         * @param suffix the suffix to use
         * @return the Builder object
         */
        public Builder withNgramSuffix(String suffix) {
            this.ngramSuffix = suffix;
            return this;
        }

        /**
         * Build a new WildcardNGramPresearcher with the supplied parameters
         * @return the constructed Presearcher
         */
        public WildcardNGramPresearcher build() {
            return new WildcardNGramPresearcher(weightor, ngramSuffix, maxTokenSize,
                    queryTreeBuilders.toArray(new QueryTreeBuilder[queryTreeBuilders.size()]));
        }

    }
}
