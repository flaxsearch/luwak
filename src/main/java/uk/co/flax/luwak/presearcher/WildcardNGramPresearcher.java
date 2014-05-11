package uk.co.flax.luwak.presearcher;

import com.google.common.collect.Lists;
import com.google.common.collect.ObjectArrays;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.KeywordRepeatFilter;
import uk.co.flax.luwak.analysis.DuplicateRemovalTokenFilter;
import uk.co.flax.luwak.analysis.SuffixingNGramTokenFilter;
import uk.co.flax.luwak.termextractor.Extractor;
import uk.co.flax.luwak.termextractor.RegexpNGramTermExtractor;
import uk.co.flax.luwak.termextractor.weights.CompoundRuleWeightor;
import uk.co.flax.luwak.termextractor.weights.TermWeightor;

import java.io.IOException;
import java.util.List;

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

    public static final String DEFAULT_NGRAM_SUFFIX = "XX";

    public static final int DEFAULT_MAX_TOKEN_SIZE = 30;

    private final String ngramSuffix;

    private final int maxTokenSize;

    /**
     * Create a new WildcardNGramPresearcher using the default QueryTermExtractor
     */
    protected WildcardNGramPresearcher(TermWeightor weightor, String ngramSuffix, int maxTokenSize,
                                       Extractor<?>... extractors) {
        super(weightor, ObjectArrays.concat(extractors, new RegexpNGramTermExtractor(ngramSuffix)));
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

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private TermWeightor weightor = CompoundRuleWeightor.DEFAULT_WEIGHTOR;
        private List<Extractor<?>> extractors = Lists.newArrayList();
        private String ngramSuffix = DEFAULT_NGRAM_SUFFIX;
        private int maxTokenSize = DEFAULT_MAX_TOKEN_SIZE;

        public Builder withWeightor(TermWeightor weightor) {
            this.weightor = weightor;
            return this;
        }

        public Builder withExtractor(Extractor<?> extractor) {
            this.extractors.add(extractor);
            return this;
        }

        public Builder withMaxTokenSize(int size) {
            this.maxTokenSize = size;
            return this;
        }

        public Builder withNgramSuffix(String suffix) {
            this.ngramSuffix = suffix;
            return this;
        }

        public WildcardNGramPresearcher build() {
            return new WildcardNGramPresearcher(weightor, ngramSuffix, maxTokenSize,
                    extractors.toArray(new Extractor[extractors.size()]));
        }

    }
}
