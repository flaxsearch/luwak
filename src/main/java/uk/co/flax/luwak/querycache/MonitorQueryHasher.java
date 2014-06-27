package uk.co.flax.luwak.querycache;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

import com.google.common.base.Charsets;
import org.apache.lucene.util.BytesRef;
import uk.co.flax.luwak.MonitorQuery;

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

public interface MonitorQueryHasher {

    public BytesRef hash(MonitorQuery query);

    public static class MD5Hasher implements MonitorQueryHasher {

        @Override
        public BytesRef hash(MonitorQuery query) {
            try {
                MessageDigest md5 = MessageDigest.getInstance("MD5");
                md5.update(query.getQuery().getBytes(Charsets.UTF_8));
                if (query.getHighlightQuery() != null)
                    md5.update(query.getHighlightQuery().getBytes(Charsets.UTF_8));
                for (Map.Entry<String, String> entry : query.getMetadata().entrySet()) {
                    md5.update(entry.getKey().getBytes(Charsets.UTF_8));
                    md5.update(entry.getValue().getBytes(Charsets.UTF_8));
                }
                return new BytesRef(md5.digest());
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException("Can't use MD5 hash on this system", e);
            }
        }
    }

}
