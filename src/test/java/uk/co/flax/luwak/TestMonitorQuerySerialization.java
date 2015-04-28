package uk.co.flax.luwak;

import com.google.common.collect.ImmutableMap;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import static uk.co.flax.luwak.TestMonitorQuerySerialization.AssertSerializes.assertThat;

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

public class TestMonitorQuerySerialization {

    public static class AssertSerializes extends AbstractAssert<AssertSerializes, MonitorQuery> {

        protected AssertSerializes(MonitorQuery actual) {
            super(actual, AssertSerializes.class);
        }

        public static AssertSerializes assertThat(MonitorQuery actual) {
            return new AssertSerializes(actual);
        }

        public AssertSerializes serializes() {
            MonitorQuery sds = MonitorQuery.deserialize(MonitorQuery.serialize(actual));
            Assertions.assertThat(sds).isEqualTo(actual);
            Assertions.assertThat(sds.hash()).isEqualTo(actual.hash());
            return this;
        }
    }

    @Test
    public void testSimpleQuery() {
        MonitorQuery mq = new MonitorQuery("1", "test");
        assertThat(mq).serializes();
    }

    @Test
    public void testQueryWithMetadata() {
        MonitorQuery mq = new MonitorQuery("1", "test", ImmutableMap.of("lang", "en", "wibble", "quack"));
        assertThat(mq).serializes();
    }

    @Test
    public void testQueryWithEmptyHighlight() {
        MonitorQuery mq = new MonitorQuery("1", "test", "");
        assertThat(mq).serializes();
    }

    @Test
    public void testMonitorQueryToString() {
        MonitorQuery mq = new MonitorQuery("1", "test");
        Assertions.assertThat(mq.toString()).isEqualTo("1: test");

        Assertions.assertThat(new MonitorQuery("1", "test", "").toString())
                .isEqualTo("1: test");
        Assertions.assertThat(new MonitorQuery("1", "test", "testhl").toString())
                .isEqualTo("1: test { hl: testhl }");
        Assertions.assertThat(new MonitorQuery("1", "test", ImmutableMap.of("lang", "en", "foo", "bar")).toString())
                .isEqualTo("1: test { foo: bar, lang: en }");
        Assertions.assertThat(new MonitorQuery("1", "test", "testhl", ImmutableMap.of("lang", "en")).toString())
                .isEqualTo("1: test { hl: testhl, lang: en }");

    }

}
