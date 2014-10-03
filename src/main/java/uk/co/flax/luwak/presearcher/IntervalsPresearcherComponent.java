package uk.co.flax.luwak.presearcher;

import java.util.List;

import com.google.common.collect.ImmutableList;
import uk.co.flax.luwak.termextractor.QueryTreeBuilder;
import uk.co.flax.luwak.termextractor.treebuilder.*;

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
 * PresearcherComponent defining QueryTreeBuilders for IntervalFilterQuery and
 * associated interval-based queries
 */
public class IntervalsPresearcherComponent extends PresearcherComponent {

    private static final List<? extends QueryTreeBuilder<?>> BUILDERS = ImmutableList.of(
            new IntervalFilterQueryTreeBuilder(),
            new NonOverlappingQueryTreeBuilder(),
            new FieldedConjunctionQueryTreeBuilder(),
            new FieldedBooleanQueryQueryTreeBuilder()
    );

    public IntervalsPresearcherComponent() {
        super(BUILDERS);
    }

}
