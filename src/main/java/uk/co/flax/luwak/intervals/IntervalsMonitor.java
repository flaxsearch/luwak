package uk.co.flax.luwak.intervals;

import org.apache.lucene.store.Directory;
import uk.co.flax.luwak.InputDocument;
import uk.co.flax.luwak.Monitor;
import uk.co.flax.luwak.MonitorQueryParser;
import uk.co.flax.luwak.Presearcher;

import java.io.IOException;

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

public class IntervalsMonitor extends Monitor {

    public IntervalsMonitor(MonitorQueryParser parser, Presearcher presearcher) throws IOException {
        super(parser, presearcher);
    }

    public IntervalsMonitor(MonitorQueryParser parser, Presearcher presearcher, Directory directory) throws IOException {
        super(parser, presearcher, directory);
    }

    public IntervalsMatcher matchWithIntervals(InputDocument doc) throws IOException {
        IntervalsMatcher matcher = new IntervalsMatcher(doc);
        this.match(matcher);
        return matcher;
    }
}
