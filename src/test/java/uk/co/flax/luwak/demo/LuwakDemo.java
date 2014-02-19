package uk.co.flax.luwak.demo;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.util.Version;
import org.fest.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.flax.luwak.*;
import uk.co.flax.luwak.impl.TermFilteredPresearcher;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
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

    public static final Analyzer ANALYZER = new StandardAnalyzer(Version.LUCENE_50);

    public static final String FIELD = "text";

    public static final Logger logger = LoggerFactory.getLogger(LuwakDemo.class);

    public static void main(String... args) throws Exception {
        new LuwakDemo("src/test/resources/demoqueries", "src/test/resources/gutenberg");
    }

    public LuwakDemo(String queriesFile, String inputDirectory) throws Exception {

        Monitor monitor = new Monitor(new TermFilteredPresearcher());
        addQueries(monitor, queriesFile);

        for (InputDocument doc : buildDocs(inputDirectory)) {
            DocumentMatches matches = monitor.match(doc);
            outputMatches(matches);
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
                QueryParser parser = new QueryParser(Version.LUCENE_50, FIELD, ANALYZER);
                queries.add(new MonitorQuery(String.format("%d-%s", count++, queryString), parser.parse(queryString)));
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
                 InputStreamReader reader = new InputStreamReader(fis)) {
                content = CharStreams.toString(reader);
                InputDocument doc = InputDocument.builder(filePath.toString())
                        .addField(FIELD, content, ANALYZER)
                        .build();
                docs.add(doc);
            }
        }
        return docs;
    }

    static void outputMatches(DocumentMatches matches) {
        logger.info("Matches from {} [{} queries run]", matches.docId(), matches.getMatchStats().querycount);
        for (QueryMatch qm : matches.matches()) {
            logger.info("\tQuery: {}", qm.getQueryId());
        }
    }

}
