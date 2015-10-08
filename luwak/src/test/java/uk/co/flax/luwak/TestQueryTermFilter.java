package uk.co.flax.luwak;

/*
 *   Copyright (c) 2015 Lemur Consulting Ltd.
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

import java.io.IOException;

import com.google.common.collect.Iterables;
import org.junit.Test;
import uk.co.flax.luwak.presearcher.TermFilteredPresearcher;
import uk.co.flax.luwak.queryparsers.LuceneQueryParser;

import static org.assertj.core.api.Assertions.assertThat;

public class TestQueryTermFilter {

    @Test
    public void testFiltersAreRemoved() throws IOException {

        try (Monitor monitor = new Monitor(new LuceneQueryParser("f"), new TermFilteredPresearcher())) {
            monitor.update(new MonitorQuery("1", "term"));
            assertThat(monitor.termFilters.size()).isEqualTo(1);
            monitor.update(new MonitorQuery("2", "term2"));
            assertThat(monitor.termFilters.size()).isEqualTo(1);

            QueryTermFilter tf = Iterables.getFirst(monitor.termFilters.values(), null);
            assertThat(tf).isNotNull();
            assertThat(tf.getTerms("f").size()).isEqualTo(2);
        }

    }

}
