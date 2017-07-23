package uk.co.flax.luwak;

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

import java.io.IOException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;


public class TestDocumentBatch {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Test
    public void emptyDocumentBatch() throws IOException {
        expected.expect(IllegalStateException.class);
        expected.expectMessage("Cannot build DocumentBatch with zero documents");

        DocumentBatch.Builder builder = new DocumentBatch.Builder();
        builder.build();
    }

}
