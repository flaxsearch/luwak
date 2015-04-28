package uk.co.flax.luwak.analysis;

import java.io.File;
import java.io.IOException;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.junit.Test;
import uk.co.flax.luwak.InputDocument;

import static uk.co.flax.luwak.util.TokenStreamAssert.assertThat;

/*
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

public class TestSuffixingNGramTokenizer {

    Analyzer analyzer = new Analyzer() {
        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
            Tokenizer source = new WhitespaceTokenizer();
            TokenStream sink = new SuffixingNGramTokenFilter(source, "XX", "ANY", 10);
            return new TokenStreamComponents(source, sink);
        }
    };

    @Test
    public void testTokensAreSuffixed() throws IOException {

        TokenStream ts = analyzer.tokenStream("f", "term");
        //TokenStreamUtils.dumpTokenStream(ts);
        assertThat(ts)
                .nextEquals("term")
                .nextEquals("termXX")
                .nextEquals("terXX")
                .nextEquals("teXX")
                .nextEquals("tXX")
                .nextEquals("ermXX")
                .nextEquals("erXX")
                .nextEquals("eXX")
                .nextEquals("rmXX")
                .nextEquals("rXX")
                .nextEquals("mXX")
                .nextEquals("XX")
                .isExhausted();

    }

    @Test
    public void testRepeatedSuffixesAreNotEmitted() throws IOException {

        TokenStream ts = analyzer.tokenStream("f", "arm harm term");
        assertThat(ts)
                .nextEquals("arm")
                .nextEquals("armXX")
                .nextEquals("arXX")
                .nextEquals("aXX")
                .nextEquals("rmXX")
                .nextEquals("rXX")
                .nextEquals("mXX")
                .nextEquals("XX")
                .nextEquals("harm")
                .nextEquals("harmXX")
                .nextEquals("harXX")
                .nextEquals("haXX")
                .nextEquals("hXX")
                .nextEquals("term")
                .nextEquals("termXX")
                .nextEquals("terXX")
                .nextEquals("teXX")
                .nextEquals("tXX")
                .nextEquals("ermXX")
                .nextEquals("erXX")
                .nextEquals("eXX")
                .isExhausted();
    }

    @Test
    public void testRepeatedInfixesAreNotEmitted() throws IOException {

        TokenStream ts = analyzer.tokenStream("f", "alarm alas harm");
        assertThat(ts)
                .nextEquals("alarm")
                .nextEquals("alarmXX").nextEquals("alarXX").nextEquals("alaXX").nextEquals("alXX").nextEquals("aXX")
                .nextEquals("larmXX").nextEquals("larXX").nextEquals("laXX").nextEquals("lXX")
                .nextEquals("armXX").nextEquals("arXX")
                .nextEquals("rmXX").nextEquals("rXX")
                .nextEquals("mXX")
                .nextEquals("XX")
                .nextEquals("alas")
                .nextEquals("alasXX")
                .nextEquals("lasXX")
                .nextEquals("asXX")
                .nextEquals("sXX")
                .nextEquals("harm")
                .nextEquals("harmXX").nextEquals("harXX").nextEquals("haXX").nextEquals("hXX")
                .isExhausted();
    }

    @Test
    public void testLengthyTokensAreNotNgrammed() throws IOException {

        TokenStream ts = analyzer.tokenStream("f", "alongtermthatshouldntbengrammed");
        assertThat(ts)
                .nextEquals("alongtermthatshouldntbengrammed")
                .nextEquals("ANY")
                .isExhausted();

    }

    public static void main(String... args) throws IOException {

        String text = Files.toString(new File("src/test/resources/gutenberg/README"), Charsets.UTF_8);
        InputDocument doc = InputDocument.builder("1")
                .addField("f", text, new WhitespaceAnalyzer()).build();

        for (int i = 0; i < 50; i++) {

            long time = System.currentTimeMillis();

            TokenStream ts = new TermsEnumTokenStream(doc.asAtomicReader().fields().terms("f").iterator(null));
            ts = new SuffixingNGramTokenFilter(ts, "XX", "__WILDCARD__", 20);
            //ts = new DuplicateRemovalTokenFilter(ts);
            int tokencount = 0;
            ts.reset();
            while (ts.incrementToken()) {
                tokencount++;
            }

            System.out.println(tokencount + " tokens in " + (System.currentTimeMillis() - time) + " ms");
        }

    }

}
