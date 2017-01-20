package uk.co.flax.luwak.server;
/*
 *   Copyright (c) 2017 Lemur Consulting Ltd.
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
import java.util.Collections;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.lucene.document.Document;
import org.junit.Test;
import uk.co.flax.luwak.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSerialization {

    private static ObjectMapper mapper = LuwakMapper.addMappings(new ObjectMapper());

    @Test
    public void testMonitorQuery() throws IOException {

        String input = "{ \"id\" : \"1\", \"query\": \"test\" }";
        MonitorQuery expected = new MonitorQuery("1", "test");

        assertThat(mapper.readValue(input, MonitorQuery.class)).isEqualTo(expected);

    }

    @Test
    public void testMonitorQueryWithMetadata() throws IOException {
        String input = "{ \"id\": \"1\", \"query\": \"test\", \"metadata\": { \"key\": \"value\", \"key2\": \"value2\" } }";
        MonitorQuery expected = new MonitorQuery("1", "test", ImmutableMap.of("key", "value", "key2", "value2"));

        assertThat(mapper.readValue(input, MonitorQuery.class)).isEqualTo(expected);
    }

    @Test
    public void testBadQuery() throws IOException {
        String input = "{ \"query\" : \"test\" }";
        assertThatThrownBy(() -> mapper.readValue(input, MonitorQuery.class)).isInstanceOf(Exception.class);
    }

    @Test
    public void testInputDocument() throws IOException {
        String input = "{ \"id\" : \"doc\", \"fields\" : { \"field\" : \"here is some text\", \"field2\" : \"wibble\" } }";
        InputDocument doc = mapper.readValue(input, InputDocument.class);

        assertThat(doc.getId()).isEqualTo("doc");
        Document luceneDoc = doc.getDocument();
        assertThat(luceneDoc.getField("field").stringValue()).isEqualTo("here is some text");
        assertThat(luceneDoc.getField("field2").stringValue()).isEqualTo("wibble");
    }

    @Test
    public void testBadDocument() throws IOException {
        String input = "{ \"fields\" : { \"field\" : \"value\"} }";
        assertThatThrownBy(() -> mapper.readValue(input, InputDocument.class))
                .isInstanceOf(Exception.class);
    }

    @Test
    public void testMatches() throws IOException {
        DocumentMatches<QueryMatch> dm = new DocumentMatches<>("doc",
                ImmutableList.of(new QueryMatch("1", "doc"), new QueryMatch("2", "doc")));

        Matches<QueryMatch> matches
                = new Matches<>(ImmutableMap.of("doc", dm), Collections.emptySet(), Collections.emptyList(), 0, 0, 0, 0, null);

        String output = mapper.writeValueAsString(matches);
        System.out.println(output);
    }

}
