package uk.co.flax.luwak;

import java.io.Closeable;
import java.io.IOException;
import java.io.StringWriter;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import javax.xml.bind.annotation.adapters.HexBinaryAdapter;

import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;

/*
 * Copyright (c) 2015 Lemur Consulting Ltd.
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
 * A Monitor contains a set of MonitorQuery objects, and runs them against
 * passed-in InputDocuments.
 */
public class DebugMonitor extends Monitor implements Closeable {

    /**
     * Create a new Monitor instance, using a passed in IndexWriter for its queryindex
     *
     * Note that when the Monitor is closed, both the IndexWriter and its underlying
     * Directory will also be closed.
     *
     * @param queryParser the query parser to use
     * @param presearcher the presearcher to use
     * @param indexWriter an indexWriter for the query index
     * @param configuration the MonitorConfiguration
     * @throws IOException on IO errors
     */
    public DebugMonitor(MonitorQueryParser queryParser, Presearcher presearcher,
                   IndexWriter indexWriter, QueryIndexConfiguration configuration) throws IOException {
        super(queryParser, presearcher, indexWriter, configuration);
    }

    /**
     * Create a new Monitor instance, using a RAMDirectory and the default configuration
     * @param queryParser the query parser to use
     * @param presearcher the presearcher to use
     * @throws IOException on IO errors
     */
    public DebugMonitor(MonitorQueryParser queryParser, Presearcher presearcher) throws IOException {
        super(queryParser, presearcher);
    }

    /**
     * Create a new Monitor instance using a RAMDirectory
     * @param queryParser the query parser to use
     * @param presearcher the presearcher to use
     * @param config the monitor configuration
     * @throws IOException on IO errors
     */
    public DebugMonitor(MonitorQueryParser queryParser, Presearcher presearcher, QueryIndexConfiguration config) throws IOException {
        super(queryParser, presearcher, config);
    }

    /**
     * Create a new Monitor instance, using the default QueryDecomposer and IndexWriter configuration
     * @param queryParser the query parser to use
     * @param presearcher the presearcher to use
     * @param directory the directory where the queryindex is stored
     * @throws IOException on IO errors
     */
    public DebugMonitor(MonitorQueryParser queryParser, Presearcher presearcher, Directory directory) throws IOException {
        super(queryParser, presearcher, directory);
    }

    /**
     * Create a new Monitor instance
     * @param queryParser the query parser to use
     * @param presearcher the presearcher to use
     * @param directory the directory where the queryindex is to be stored
     * @param config the monitor configuration
     * @throws IOException on IO errors
     */
    public DebugMonitor(MonitorQueryParser queryParser, Presearcher presearcher, Directory directory, QueryIndexConfiguration config) throws IOException {
        super(queryParser, presearcher, directory, config);
    }

    /**
     * Create a new Monitor instance, using the default QueryDecomposer
     * @param queryParser the query parser to use
     * @param presearcher the presearcher to use
     * @param indexWriter a {@link IndexWriter} for the Monitor's query index
     * @throws IOException on IO errors
     */
    public DebugMonitor(MonitorQueryParser queryParser, Presearcher presearcher, IndexWriter indexWriter) throws IOException {
        super(queryParser, presearcher, indexWriter);
    }

    public interface MonitorQueryListener {
        boolean onMonitorQuery(MonitorQuery mq);
    }

    public void iterateAllQueries(final MonitorQueryListener listener) throws IOException {
        final AtomicInteger finished = new AtomicInteger();

        final Set<String> seenBefore = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

        queryIndex.scan(new QueryIndex.QueryCollector() {
            @Override
            public void matchQuery(String id, QueryCacheEntry query, QueryIndex.DataValues dataValues) throws IOException {

                if (finished.get() != 0) {
                    return;
                }

                final BytesRef serializedMQ = dataValues.mq.get(dataValues.doc);
                MonitorQuery mq = MonitorQuery.deserialize(serializedMQ);

                if (!seenBefore.contains(id)) {
                    seenBefore.add(id);

                    if (!listener.onMonitorQuery(mq)) {
                        finished.set(1);
                    }
                }
            }
        });
    }

    public String debugSubscription(String id) throws IOException {
        final StringBuilder sb = new StringBuilder();

        BooleanQuery.Builder builder = new BooleanQuery.Builder();

        builder.add(new TermQuery(new Term(FIELDS.id, id)), BooleanClause.Occur.MUST);

        queryIndex.search(builder.build(), new QueryIndex.QueryCollector() {
            @Override
            public void matchQuery(String queryid, QueryCacheEntry entry, QueryIndex.DataValues dataValues) throws IOException {
                sb.append("doc: ");
                sb.append(String.valueOf(dataValues.doc));
                sb.append("\n");

                sb.append("hash:");
                sb.append(String.valueOf(dataValues.hash.get(dataValues.doc)));
                sb.append("\n");

                sb.append("cache entry ");
                if (entry != null) {
                    sb.append("found: {");
                    sb.append(entry.matchQuery);
                    sb.append(", ");
                    sb.append(entry.metadata);
                    sb.append("}");
                } else {
                    sb.append("not found");
                }
                sb.append("\n");

                sb.append("fields:\n");
                Document document = dataValues.reader.document(dataValues.doc);
                for (IndexableField field : document.getFields()) {
                    sb.append(field.name());
                    sb.append(":");
                    String[] tokens = field.stringValue() == null ? new String[0] : field.stringValue().split(" ");
                    Arrays.sort(tokens);
                    for (String i: tokens) {
                        sb.append(" ");
                        sb.append(i);
                    }
                    sb.append("\n");
                }
            }
        });

        return sb.toString();
    }

    public SortedMap<String, String> indexChecksums() throws IOException {
        final class MessagePacker {
            MessageDigest md;
            Exception e;
        }

        final SortedMap<String, MessagePacker> digests = new TreeMap<String, MessagePacker>();

        // No need for the atomic part. It's just a mutable final reference.
        final AtomicReference<NoSuchAlgorithmException> noMd5Exception = new AtomicReference<NoSuchAlgorithmException>();

        queryIndex.search(new MatchAllDocsQuery(), new QueryIndex.QueryCollector() {
            @Override
            public void matchQuery(String id, QueryCacheEntry query, QueryIndex.DataValues dataValues) throws IOException {

                // If there was an md5 exception, then there's nothing more to do.
                if (noMd5Exception.get() != null) {
                    return;
                }

                // Unpack the digest and exception for this query
                MessagePacker mp = digests.get(id);
                if (mp == null) {
                    mp = new MessagePacker();
                    digests.put(id, mp);
                }

                // If there was an exception for this query, then
                // stop processing.
                if (mp.e != null) {
                    return;
                }

                // Handle potential md5 creation...
                MessageDigest md5 = mp.md;
                if (md5 == null)
                {
                    try {
                        // With 300k md5s overall and e.g. 256 bytes per md5
                        // we should need less than 100Mb for these.
                        mp.md = MessageDigest.getInstance("MD5");
                        md5 = mp.md;
                    } catch (NoSuchAlgorithmException e) {
                        // If we don't have md5 there isn't much
                        // this function can do
                        noMd5Exception.set(e);
                        return;
                    }
                }

                try {  // I would like the dump to continue even in case there's an exception here.
                    if (query != null) {
                        md5.update(query.hash.bytes);

                        // those xQuery's could be null
                        md5.update(String.valueOf(query.matchQuery).getBytes(StandardCharsets.UTF_8));
                        md5.update(String.valueOf(query.metadata).getBytes(StandardCharsets.UTF_8));
                    }

                    Document document =  dataValues.reader.document(dataValues.doc);
                    for (IndexableField field : document.getFields()) {
                        md5.update(field.name().getBytes(StandardCharsets.UTF_8));
                        String[] tokens = field.stringValue() == null ? new String[0] : field.stringValue().split(" ");
                        Arrays.sort(tokens);
                        for (String i: tokens) {
                            md5.update(i.getBytes(StandardCharsets.UTF_8));
                        }
                    }
                }
                catch (Exception e)
                {
                    // If there was an exception, keep track of it and stop
                    // processing for this query.
                    mp.e = e;
                }
            }
        });

        if (noMd5Exception.get() != null) {
            throw new RuntimeException("Can't use MD5 hash on this system", noMd5Exception.get());
        }

        HexBinaryAdapter formatter = new HexBinaryAdapter();

        SortedMap<String, String> checksums = new TreeMap<String, String>();
        for (Map.Entry<String, MessagePacker> digest : digests.entrySet()) {
            MessagePacker mp = digest.getValue();
            if (mp.e != null) {
                // There was an exception for this query. Print it.
                StringWriter sw = new StringWriter();
                mp.e.printStackTrace(new PrintWriter(sw));
                checksums.put(digest.getKey(), sw.toString());
            }
            else {
                // Print the digest
                MessageDigest md5 = mp.md;
                checksums.put(digest.getKey(), formatter.marshal(md5.digest()));
            }
        }

        return checksums;
    }
}
