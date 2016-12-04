package uk.co.flax.luwak.presearcher;
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

import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.apache.lucene.search.Query;
import org.junit.Test;
import org.reflections.Reflections;
import uk.co.flax.luwak.termextractor.QueryAnalyzer;
import uk.co.flax.luwak.termextractor.treebuilder.GenericQueryTreeBuilder;

import static org.assertj.core.api.Assertions.assertThat;

public class TestLuceneQueries {

    public static Set<String> unhandledQueries = ImmutableSet.of(
            "org.apache.lucene.search.DocValuesRangeQuery"
    );

    @Test
    public void checkAllCoreQueriesAreHandled() {

        Reflections reflections = new Reflections("org.apache.lucene");
        Set<Class<? extends Query>> coreQueries = reflections.getSubTypesOf(Query.class);

        QueryAnalyzer defaultAnalyzer = QueryAnalyzer.fromComponents(new WildcardNGramPresearcherComponent());

        Set<String> missingClasses = new HashSet<>();

        for (Class<? extends Query> c : coreQueries) {
            if (Modifier.isAbstract(c.getModifiers()))
                continue;
            if (defaultAnalyzer.getTreeBuilderForQuery(c) instanceof GenericQueryTreeBuilder) {
                String className = c.getName();
                if (unhandledQueries.contains(className) == false)
                    missingClasses.add(className);
            }
        }

        assertThat(missingClasses).isEmpty();

    }

}
