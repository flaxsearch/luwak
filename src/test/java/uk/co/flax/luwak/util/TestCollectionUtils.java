package uk.co.flax.luwak.util;

import java.util.List;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

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

public class TestCollectionUtils {

    @Test
    public void testPartitions() {

        List<String> terms = list("1", "2", "3", "4", "5", "6", "7", "8", "9", "10");
        assertThat(CollectionUtils.partition(terms, 2))
                .containsExactly(list("1", "2", "3", "4", "5"), list("6", "7", "8", "9", "10"));

        assertThat(CollectionUtils.partition(terms, 3))
                .containsExactly(list("1", "2", "3"), list("4", "5", "6"), list("7", "8", "9", "10"));

        assertThat(CollectionUtils.partition(terms, 4))
                .containsExactly(list("1", "2"), list("3", "4", "5"), list("6", "7"), list("8", "9", "10"));

        assertThat(CollectionUtils.partition(terms, 6))
                .containsExactly(list("1"), list("2", "3"), list("4", "5"), list("6"), list("7", "8"), list("9", "10"));

    }

    public static List<String> list(String... terms) {
        return ImmutableList.copyOf(terms);
    }

}
