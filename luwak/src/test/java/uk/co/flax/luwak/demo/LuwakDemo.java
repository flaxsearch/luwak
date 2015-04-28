package uk.co.flax.luwak.demo;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.io.CharStreams;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.flax.luwak.*;
import uk.co.flax.luwak.matchers.SimpleMatcher;
import uk.co.flax.luwak.presearcher.TermFilteredPresearcher;
import uk.co.flax.luwak.queryparsers.LuceneQueryParser;

/*
 * Copyright (c) 2013 Lemur Consulting Ltd.
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

public class LuwakDemo {

    public static final Analyzer ANALYZER = new StandardAnalyzer();

    public static final String FIELD = "text";

    public static final Logger logger = LoggerFactory.getLogger(LuwakDemo.class);

    public static void main(String... args) throws Exception {
        new LuwakDemo("src/test/resources/demoqueries", "src/test/resources/gutenberg");
    }

    public LuwakDemo(String queriesFile, String inputDirectory) throws Exception {

        try (Monitor monitor = new Monitor(new LuceneQueryParser(FIELD, ANALYZER), new TermFilteredPresearcher())) {
            addQueries(monitor, queriesFile);

            for (InputDocument doc : buildDocs(inputDirectory)) {
                Matches<QueryMatch> matches = monitor.match(doc, SimpleMatcher.FACTORY);
                outputMatches(matches);
            }
        }

    }

    static void addQueries(Monitor monitor, String queriesFile) throws Exception {
        List<MonitorQuery> queries = new ArrayList<>();
        int count = 0;
        logger.info("Loading queries from {}", queriesFile);
        try (FileInputStream fis = new FileInputStream(queriesFile);
                BufferedReader br = new BufferedReader(new InputStreamReader(fis,Charsets.UTF_8))) {
            String queryString;
            while ((queryString = br.readLine()) != null) {
                if (Strings.isNullOrEmpty(queryString))
                    continue;
                logger.info("Parsing [{}]", queryString);
                queries.add(new MonitorQuery(String.format(Locale.ROOT, "%d-%s", count++, queryString), queryString));
            }
        }
        monitor.update(queries);
        logger.info("Added {} queries to monitor", count);
    }

    static List<InputDocument> buildDocs(String inputDirectory) throws Exception {
        List<InputDocument> docs = new ArrayList<>();
        logger.info("Reading documents from {}", inputDirectory);
        for (Path filePath : Files.newDirectoryStream(FileSystems.getDefault().getPath(inputDirectory))) {
            String content;
            try (FileInputStream fis = new FileInputStream(filePath.toFile());
                 InputStreamReader reader = new InputStreamReader(fis, Charsets.UTF_8)) {
                content = CharStreams.toString(reader);
                InputDocument doc = InputDocument.builder(filePath.toString())
                        .addField(FIELD, content, ANALYZER)
                        .build();
                docs.add(doc);
            }
        }
        return docs;
    }

    static void outputMatches(Matches<QueryMatch> matches) {
        logger.info("Matches from {} [{} queries run]", matches.docId(), matches.getQueriesRun());
        for (QueryMatch query : matches) {
            logger.info("\tQuery: {}", query.getQueryId());
        }
    }

}
