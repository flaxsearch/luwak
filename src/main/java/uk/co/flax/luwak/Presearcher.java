package uk.co.flax.luwak;

import org.apache.lucene.document.Document;
import org.apache.lucene.search.Query;

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
 * Abstract base class of all Presearcher implementations
 *
 * A Presearcher is used by the Monitor to reduce the number of queries actually
 * run against an InputDocument.  It defines how queries are stored in the monitor's
 * internal index, and how an InputDocument is converted to a query against that
 * index.
 *
 * {@see uk.co.flax.luwak.impl.MatchAllPresearcher}
 * {@see uk.co.flax.luwak.impl.TermFilteredPresearcher}
 * {@see uk.co.flax.luwak.impl.WildcardNGramPresearcher}
 */
public abstract class Presearcher {

    protected Monitor monitor;

    protected void setMonitor(Monitor monitor) {
        this.monitor = monitor;
    }

    public abstract Query buildQuery(InputDocument inputDocument);

    public abstract void indexQuery(Document doc, Query query);

    public Monitor getMonitor() {
        return monitor;
    }

}
