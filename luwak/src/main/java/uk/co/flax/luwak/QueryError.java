package uk.co.flax.luwak;

import java.util.Locale;

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

/**
 * Represents an error due to parsing or indexing a query
 */
public class QueryError {

    /** The query */
    public final MonitorQuery query;

    /** The error */
    public final Exception error;

    /**
     * Create a new QueryError
     * @param query the query
     * @param error the error
     */
    public QueryError(MonitorQuery query, Exception error) {
        this.query = query;
        this.error = error;
    }

    @Override
    public String toString() {
        return String.format(Locale.ROOT, "Error parsing query %s : %s", query, error.getMessage());
    }
}
