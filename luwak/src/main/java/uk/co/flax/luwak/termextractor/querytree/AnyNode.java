package uk.co.flax.luwak.termextractor.querytree;

import uk.co.flax.luwak.termextractor.QueryTerm;

/*
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

public class AnyNode extends TermNode {

    public AnyNode(QueryTerm term) {
        super(term);
    }

    public AnyNode(String reason) {
        this(new QueryTerm("", reason, QueryTerm.Type.ANY));
    }

    public AnyNode(String field, String reason) {
        this(new QueryTerm(field, reason, QueryTerm.Type.ANY));
    }

    @Override
    public String toString(TreeWeightor weightor, TreeAdvancer advancer) {
        return "AnyNode [" + term.toString() + "] " + this.weight(weightor);
    }

    @Override
    public boolean isAny() {
        return true;
    }

    @Override
    public float weight(TreeWeightor weightor) {
        return 0;
    }
}
