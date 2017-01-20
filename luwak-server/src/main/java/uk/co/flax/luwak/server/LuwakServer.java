package uk.co.flax.luwak.server;
/*
 *   Copyright (c) 2017 Lemur Consulting Ltd.
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

import io.dropwizard.Application;
import io.dropwizard.setup.Environment;
import uk.co.flax.luwak.server.resources.MatchResource;
import uk.co.flax.luwak.server.resources.UpdateResource;

public class LuwakServer extends Application<LuwakConfiguration> {

    public static void main(String[] args) throws Exception {
        // If no arguments are given, assume we just want to start up the server
        if (args.length == 0) {
            args = new String[] {"server"};
        }
        new LuwakServer().run(args);
    }

    @Override
    public void run(LuwakConfiguration luwakConfiguration, Environment environment) throws Exception {
        environment.jersey().register(new MatchResource());
        environment.jersey().register(new UpdateResource());
    }
}
