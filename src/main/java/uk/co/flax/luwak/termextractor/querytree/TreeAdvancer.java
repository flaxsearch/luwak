package uk.co.flax.luwak.termextractor.querytree;

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
 * Used to decide a new route through a {@link QueryTree}
 *
 * Conjunction nodes can be indexed equally by any of their children, but when
 * using a {@link uk.co.flax.luwak.presearcher.MultipassTermFilteredPresearcher}
 * we don't necessarily want to select certain children to be indexed - for example,
 * ANY tokens or very high frequency terms will end up producing many false
 * positive matches.
 *
 * TreeAdvancer exposes a single method, {@link TreeAdvancer#canAdvanceOver(QueryTree)},
 * which determines whether or not a given QueryTree is a suitable candidate to be
 * indexed when the phase is advanced.
 */
public abstract class TreeAdvancer {

    public static final TreeAdvancer NOOP = new NoOpTreeAdvancer();

    /**
     * Should this tree be selected for indexing in the next phase?
     *
     * @param child the QueryTree to check
     * @return true if the child can be advanced over
     */
    public abstract boolean canAdvanceOver(QueryTree child);

    /**
     * Never allows advancing
     */
    public static class NoOpTreeAdvancer extends TreeAdvancer {

        @Override
        public boolean canAdvanceOver(QueryTree child) {
            return false;
        }
    }

    /**
     * Only advance if the child has a weight greater than a minimum value
     */
    public static class MinWeightTreeAdvancer extends TreeAdvancer {

        public final float minWeight;

        public final TreeWeightor weightor;

        /**
         * Create a new MinWeightTreeAdvancer
         * @param weightor the TreeWeightor to use to calculate child weights
         * @param minWeight the minimum weight to advance over
         */
        public MinWeightTreeAdvancer(TreeWeightor weightor, float minWeight) {
            this.minWeight = minWeight;
            this.weightor = weightor;
        }

        @Override
        public boolean canAdvanceOver(QueryTree child) {
            return child.weight(weightor) > minWeight;
        }
    }
}
