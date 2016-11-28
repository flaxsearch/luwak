package uk.co.flax.luwak;

/*
 *   Copyright (c) 2016 Lemur Consulting Ltd.
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

import java.util.List;

/**
 * Thrown by {@link Monitor#update(Iterable)} if an Exception is thrown while any of
 * the queries are added.
 */
public class UpdateException extends Exception {

    private static final long serialVersionUID = -2701284159561061331L;

    /**
     * The list of errors thrown during an update
     */
    public final List<QueryError> errors;

    public UpdateException(List<QueryError> errors) {
        this.errors = errors;
    }
}
