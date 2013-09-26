package uk.co.flax.luwak;

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import org.apache.lucene.search.intervals.Interval;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

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

public class QueryMatch {

    private final String queryId;
    private final Multimap<String, Hit> hits = TreeMultimap.<String, Hit>create();

    public QueryMatch(String queryId) {
        this.queryId = queryId;
    }

    public void addInterval(Interval interval) {
        hits.put(interval.field, new Hit(interval.begin, interval.offsetBegin, interval.end, interval.offsetEnd));
    }

    public String getQueryId() {
        return this.queryId;
    }

    public Set<String> getFields() {
        return hits.keySet();
    }

    public Collection<Hit> getHits(String field) {
        return Collections.unmodifiableCollection(hits.get(field));
    }

    public int getHitCount() {
        return hits.keys().size();
    }

    public static class Hit implements Comparable<Hit> {

        public final int startPosition;
        public final int startOffset;
        public final int endPosition;
        public final int endOffset;

        Hit(int startPosition, int startOffset, int endPosition, int endOffset) {
            this.startPosition = startPosition;
            this.startOffset = startOffset;
            this.endPosition = endPosition;
            this.endOffset = endOffset;
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof Hit))
                return false;
            Hit other = (Hit) obj;
            return this.startOffset == other.startOffset &&
                    this.endOffset == other.endOffset &&
                    this.startPosition == other.startPosition &&
                    this.endPosition == other.endPosition;
        }

        @Override
        public String toString() {
            return String.format("%d(%d)->%d(%d)", startPosition, startOffset, endPosition, endOffset);
        }

        @Override
        public int compareTo(Hit other) {
            if (this.startPosition != other.startPosition)
                return Integer.compare(this.startPosition, other.startPosition);
            return Integer.compare(this.endPosition, other.endPosition);
        }
    }
}
