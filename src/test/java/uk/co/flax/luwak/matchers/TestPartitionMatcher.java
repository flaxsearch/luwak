package uk.co.flax.luwak.matchers;

import java.util.concurrent.ExecutorService;

import uk.co.flax.luwak.MatcherFactory;
import uk.co.flax.luwak.QueryMatch;

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

public class TestPartitionMatcher extends ConcurrentMatcherTestBase {

    @Override
    protected <T extends QueryMatch> MatcherFactory<T> matcherFactory(ExecutorService executor, MatcherFactory<T> factory, int threads) {
        return PartitionMatcher.factory(executor, factory, threads);
    }
}
