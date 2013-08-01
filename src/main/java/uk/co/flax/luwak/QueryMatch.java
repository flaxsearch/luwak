package uk.co.flax.luwak;

import org.apache.lucene.search.intervals.Interval;

import java.util.ArrayList;
import java.util.Collections;
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

public class QueryMatch {

    private final String queryId;
    private final List<Hit> hits = new ArrayList<>();

    public QueryMatch(String queryId) {
        this.queryId = queryId;
    }

    public void addInterval(Interval interval) {
        hits.add(new Hit(interval.begin, interval.offsetBegin, interval.end, interval.offsetEnd));
    }

    public String getQueryId() {
        return this.queryId;
    }

    public List<Hit> getHits() {
        return Collections.unmodifiableList(hits);
    }

    public static class Hit {

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

    }
}
