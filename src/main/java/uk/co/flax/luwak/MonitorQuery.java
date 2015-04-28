package uk.co.flax.luwak;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.BytesRef;

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
 * Defines a query to be stored in a Monitor
 */
public class MonitorQuery {

    private final String id;
    private final String query;
    private final String highlightQuery;
    private final Map<String, String> metadata;

    /**
     * Creates a new MonitorQuery
     * @param id the ID
     * @param query the query to store
     * @param highlightQuery an optional query to use for highlighting.  May be null.
     * @param metadata metadata passed to {@link Presearcher#indexQuery(org.apache.lucene.search.Query,java.util.Map)}
     */
    public MonitorQuery(String id, String query, String highlightQuery, Map<String, String> metadata) {
        this.id = id;
        this.query = query;
        this.highlightQuery = (highlightQuery == null || highlightQuery.isEmpty()) ? null : highlightQuery;
        this.metadata = new TreeMap<>(metadata);
    }

    /**
     * Creates a new MonitorQuery with no highlight query
     * @param id the ID
     * @param query the query to store
     * @param metadata metadata passed to {@link Presearcher#indexQuery(org.apache.lucene.search.Query,java.util.Map)}
     */
    public MonitorQuery(String id, String query, Map<String, String> metadata) {
        this(id, query, null, metadata);
    }

    /**
     * Creates a new MonitorQuery with empty metadata
     * @param id the ID
     * @param query the query
     * @param highlight the highlight query
     */
    public MonitorQuery(String id, String query, String highlight) {
        this(id, query, highlight, new HashMap<String, String>());
    }

    /**
     * Creates a new MonitorQuery, with no highlight query and no metadata
     * @param id the ID
     * @param query the query to store
     */
    public MonitorQuery(String id, String query) {
        this(id, query, null, new HashMap<String, String>());
    }

    /**
     * Deserialize a MonitorQuery from a stream of bytes
     * @param bytes a BytesRef pointing to the serialized query
     * @return the deserialized MonitorQuery
     */
    public static MonitorQuery deserialize(BytesRef bytes)  {

        ByteArrayInputStream is = new ByteArrayInputStream(bytes.bytes);
        try (InputStreamDataInput data = new InputStreamDataInput(is)) {

            String id = data.readString();
            String query = data.readString();
            String hl = data.readInt() == 1 ? data.readString() : null;
            Map<String, String> metadata = new HashMap<>();
            for (int i = data.readInt(); i > 0; i--) {
                metadata.put(data.readString(), data.readString());
            }
            return new MonitorQuery(id, query, hl, metadata);

        } catch (IOException e) {
            throw new RuntimeException(e);  // shouldn't happen, we're reading from a bytearray!
        }

    }

    /**
     * Serialize a MonitorQuery into a BytesRef
     * @param mq the MonitorQuery
     * @return the serialized bytes
     */
    public static BytesRef serialize(MonitorQuery mq) {

        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try (OutputStreamDataOutput data = new OutputStreamDataOutput(os)) {

            data.writeString(mq.getId());
            data.writeString(mq.getQuery());
            if (mq.getHighlightQuery() != null) {
                data.writeInt(1);
                data.writeString(mq.getHighlightQuery());
            }
            else {
                data.writeInt(0);
            }
            data.writeInt(mq.getMetadata().size());
            for (Map.Entry<String, String> entry : mq.getMetadata().entrySet()) {
                data.writeString(entry.getKey());
                data.writeString(entry.getValue());
            }
            return new BytesRef(os.toByteArray());

        } catch (IOException e) {
            throw new RuntimeException(e); // shouldn't happen, we're writing to a bytearray!
        }

    }

    /**
     * @return this MonitorQuery's ID
     */
    public String getId() {
        return id;
    }

    /**
     * @return this MonitorQuery's query
     */
    public String getQuery() {
        return query;
    }

    /**
     * @return this MonitorQuery's highlight query.  May be null.
     */
    public String getHighlightQuery() {
        return highlightQuery;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MonitorQuery that = (MonitorQuery) o;

        if (highlightQuery != null ? !highlightQuery.equals(that.highlightQuery) : that.highlightQuery != null)
            return false;
        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        if (query != null ? !query.equals(that.query) : that.query != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (query != null ? query.hashCode() : 0);
        result = 31 * result + (highlightQuery != null ? highlightQuery.hashCode() : 0);
        return result;
    }

    public BytesRef hash() {
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            md5.update(query.getBytes(StandardCharsets.UTF_8));
            if (highlightQuery != null)
                md5.update(highlightQuery.getBytes(StandardCharsets.UTF_8));
            for (Map.Entry<String, String> entry : metadata.entrySet()) {
                md5.update(entry.getKey().getBytes(StandardCharsets.UTF_8));
                md5.update(entry.getValue().getBytes(StandardCharsets.UTF_8));
            }
            return new BytesRef(md5.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Can't use MD5 hash on this system", e);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(id);
        sb.append(": ").append(query);
        if (highlightQuery != null || metadata.size() != 0) {
            sb.append(" { ");
            if (highlightQuery != null) {
                sb.append("hl: ").append(highlightQuery);
                if (metadata.size() != 0)
                    sb.append(", ");
            }
            int n = metadata.size();
            for (Map.Entry<String, String> entry : metadata.entrySet()) {
                n--;
                sb.append(entry.getKey()).append(": ").append(entry.getValue());
                if (n > 0)
                    sb.append(", ");
            }
            sb.append(" }");
        }
        return sb.toString();
    }
}
