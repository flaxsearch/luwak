package uk.co.flax.luwak.termextractor;
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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.google.common.collect.ImmutableSet;
import org.apache.lucene.search.Query;
import org.junit.Test;
import org.reflections.Reflections;
import org.slf4j.LoggerFactory;
import uk.co.flax.luwak.presearcher.WildcardNGramPresearcherComponent;
import uk.co.flax.luwak.termextractor.treebuilder.TreeBuilders;

import static org.assertj.core.api.Assertions.assertThat;

public class TestCoreLuceneQueryExtractors {

    public static Set<String> todoQueries = ImmutableSet.of(
            "org.apache.lucene.search.SynonymQuery",
            "org.apache.lucene.search.WildcardQuery",
            "org.apache.lucene.search.NGramPhraseQuery",
            "org.apache.lucene.search.MultiPhraseQuery",
            "org.apache.lucene.queries.CustomScoreQuery",
            "org.apache.lucene.search.PrefixQuery",
            "org.apache.lucene.queries.CommonTermsQuery",
            "org.apache.lucene.search.FieldValueQuery",
            "org.apache.lucene.search.BlendedTermQuery",
            "org.apache.lucene.search.AutomatonQuery",
            "org.apache.lucene.queries.BoostingQuery"
    );

    public static Set<String> unhandledQueries = ImmutableSet.of(
            "org.apache.lucene.search.DocValuesRangeQuery",
            "org.apache.lucene.search.MatchNoDocsQuery",
            "org.apache.lucene.search.MatchAllDocsQuery",
            "org.apache.lucene.search.DocValuesTermsQuery",
            "org.apache.lucene.search.TermRangeQuery",
            "org.apache.lucene.search.TermAutomatonQuery",
            "org.apache.lucene.search.DocValuesNumbersQuery",
            "org.apache.lucene.sandbox.queries.SlowFuzzyQuery",
            "org.apache.lucene.queries.function.FunctionRangeQuery",
            "org.apache.lucene.document.LatLonPointDistanceQuery",
            "org.apache.lucene.sandbox.queries.FuzzyLikeThisQuery",
            "org.apache.lucene.search.FuzzyQuery",
            "org.apache.lucene.queries.mlt.MoreLikeThisQuery",
            "org.apache.lucene.queries.function.FunctionQuery",
            "org.apache.lucene.queries.function.FunctionMatchQuery",
            "org.apache.lucene.search.IndexOrDocValuesQuery",
            "org.apache.lucene.search.LegacyNumericRangeQuery"
    );

    public static Set<String> unhandledTypes = ImmutableSet.of(
            "org.apache.lucene.search.PointRangeQuery",
            "org.apache.lucene.search.PointInSetQuery",
            "org.apache.lucene.document.RangeFieldQuery"
    );

    private static <T extends Query> boolean shouldHandle(Class<T> type) {
        if (unhandledQueries.contains(type.getName()))
            return false;
        if (todoQueries.contains(type.getName()))
            return false;
        return shouldHandleParent(type);
    }

    private static boolean shouldHandleParent(Class<?> type) {
        while (type.getSuperclass() != Object.class) {
            if (unhandledTypes.contains(type.getName()))
                return false;
            type = type.getSuperclass();
        }
        return true;
    }

    private static void quietLogging() {
        Logger logger = (Logger) LoggerFactory.getLogger("org.reflections.Reflections");
        logger.setLevel(Level.ERROR);
    }

    @Test
    public void checkAllCoreQueriesAreHandled() {

        quietLogging();

        Reflections reflections = new Reflections("org.apache.lucene");
        Set<Class<? extends Query>> coreQueries = reflections.getSubTypesOf(Query.class);

        QueryAnalyzer defaultAnalyzer = QueryAnalyzer.fromComponents(new WildcardNGramPresearcherComponent());

        Set<String> missingClasses = new HashSet<>();

        for (Class<? extends Query> c : coreQueries) {
            if (Modifier.isAbstract(c.getModifiers()))
                continue;
            if (Modifier.isPublic(c.getModifiers()) != true)
                continue;
            if (defaultAnalyzer.getTreeBuilderForQuery(c) == TreeBuilders.ANY_NODE_BUILDER) {
                if (shouldHandle(c))
                    missingClasses.add(c.getName());
            }
        }

        assertThat(missingClasses).isEmpty();

    }

}
