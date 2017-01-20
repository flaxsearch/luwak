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
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import uk.co.flax.luwak.*;

public class LuwakMapper extends SimpleModule {

    public static ObjectMapper addMappings(ObjectMapper in) {
        in.registerModule(INSTANCE);
        return in;
    }

    private static final LuwakMapper INSTANCE = new LuwakMapper();

    private LuwakMapper() {
        super("LuwakMapper");
        addDeserializer(MonitorQuery.class, new MonitorQueryDeserializer());
        addDeserializer(InputDocument.class, new InputDocumentDeserializer());
        addSerializer(Matches.class, new MatchesSerializer());
    }

    private static class MatchesSerializer extends JsonSerializer<Matches> {

        // TODO: stats, errors, more or less everything...

        @Override
        public void serialize(Matches dm, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException, JsonProcessingException {
            jsonGenerator.writeStartObject();
            Matches<QueryMatch> documentMatches = (Matches<QueryMatch>) dm;
            for (DocumentMatches<QueryMatch> doc : documentMatches) {
                jsonGenerator.writeStringField("doc", doc.getDocId());
                jsonGenerator.writeFieldName("matches");
                jsonGenerator.writeStartArray();
                for (QueryMatch qm : doc) {
                    jsonGenerator.writeString(qm.getQueryId());
                }
                jsonGenerator.writeEndArray();
            }
            jsonGenerator.writeEndObject();
        }
    }

    private static class InputDocumentDeserializer extends JsonDeserializer<InputDocument> {

        @Override
        public InputDocument deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            String id = null;
            Map<String, String> fields = Collections.emptyMap();
            while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
                String fieldname = jsonParser.getCurrentName();
                switch (fieldname) {
                    case "id":
                        id = jsonParser.getText();
                        break;
                    case "fields":
                        fields = readMap(jsonParser);
                        break;
                }
            }
            Analyzer analyzer = new StandardAnalyzer();
            InputDocument.Builder builder = new InputDocument.Builder(id);
            for (Map.Entry<String, String> entry : fields.entrySet()) {
                builder.addField(entry.getKey(), entry.getValue(), analyzer);
            }
            return builder.build();
        }

    }

    private static class MonitorQueryDeserializer extends JsonDeserializer<MonitorQuery> {
        @Override
        public MonitorQuery deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            String id = null;
            String query = null;
            Map<String, String> metadata = Collections.emptyMap();
            while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
                String fieldname = jsonParser.getCurrentName();
                switch (fieldname) {
                    case "id":
                        id = jsonParser.getText();
                        break;
                    case "query":
                        query = jsonParser.getText();
                        break;
                    case "metadata":
                        metadata = readMap(jsonParser);
                        break;
                }
            }
            return new MonitorQuery(id, query, metadata);
        }

    }

    private static Map<String, String> readMap(JsonParser jp) throws IOException {
        Map<String, String> metadata = new HashMap<>();
        while (jp.nextToken() != JsonToken.END_OBJECT) {
            metadata.put(jp.getCurrentName(), jp.getText());
        }
        return metadata;
    }
}
