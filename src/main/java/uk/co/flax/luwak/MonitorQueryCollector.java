package uk.co.flax.luwak;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

public class MonitorQueryCollector extends Collector {

    static {
        BooleanQuery.setMaxClauseCount(10000);
    }

    private final InputDocument doc;
    private final Map<String, MonitorQuery> queries;

    private final List<QueryMatch> matches = new ArrayList<QueryMatch>();

    SortedDocValues idField;
    final BytesRef idRef = new BytesRef();

    //IndexSearcher withinDocSearcher;

    private int queryCount;

    public MonitorQueryCollector(Map<String, MonitorQuery> queries, final InputDocument doc) {
        this.doc = doc;
        this.queries = queries;
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
        // no impl
    }

    @Override
    public void collect(final int docnum) throws IOException {

        idField.get(docnum, idRef);
        final MonitorQuery mq = queries.get(idRef.utf8ToString());

        QueryMatch matches = doc.search(mq);
        if (matches != null)
            this.matches.add(matches);

        queryCount++;

    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
        idField = context.reader().getSortedDocValues(Monitor.FIELDS.id);
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return true;
    }

    public DocumentMatches getMatches(long preptime, long querytime) {
        return new DocumentMatches(this.doc.getId(), this.matches, this.queryCount, preptime, querytime);
    }

}