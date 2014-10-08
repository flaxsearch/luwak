package uk.co.flax.luwak.presearcher;

import java.io.IOException;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import uk.co.flax.luwak.termextractor.QueryAnalyzer;
import uk.co.flax.luwak.termextractor.QueryTerm;

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

public class DefaultPresearcherComponent extends PresearcherComponent {

    public static final String DEFAULT_ANYTOKEN = "__ANYTOKEN__";

    public final String anytoken;

    public DefaultPresearcherComponent(String anytoken) {
        super(QueryAnalyzer.DEFAULT_BUILDERS);
        this.anytoken = anytoken;
    }

    public DefaultPresearcherComponent() {
        this(DEFAULT_ANYTOKEN);
    }



    @Override
    public TokenStream filterDocumentTokens(String field, TokenStream ts) {
        return new TokenFilter(ts) {

            boolean finished = false;
            CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

            @Override
            public final boolean incrementToken() throws IOException {
                if (input.incrementToken())
                    return true;
                if (finished)
                    return false;
                finished = true;
                termAtt.setEmpty().append(anytoken);
                return true;
            }
        };
    }

    @Override
    public String extraToken(QueryTerm term) {
        if (term.type == QueryTerm.Type.ANY)
            return anytoken;
        return null;
    }
}
