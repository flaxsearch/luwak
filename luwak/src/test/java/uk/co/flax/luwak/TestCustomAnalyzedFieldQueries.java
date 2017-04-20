package uk.co.flax.luwak;

import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.junit.Assert;
import org.junit.Test;
import uk.co.flax.luwak.matchers.ExplainingMatch;
import uk.co.flax.luwak.matchers.ExplainingMatcher;
import uk.co.flax.luwak.presearcher.TermFilteredPresearcher;
import uk.co.flax.luwak.queryparsers.LuceneQueryParser;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashMap;

/**
 * Created by edvorg on 16/09/25.
 * date field query in luwak/lucene
 * ip field query in luwak/lucene
 */

public class TestCustomAnalyzedFieldQueries {
    private static final String defaultField = "field";
    private static final String nameField = "name";
    private static final String docId = "doc-id";
    private static final String queryId = "query-id";

    public static long parseIp(String ip) {
        String[] parts = ip.split("\\.");
        long result = Long.parseLong(parts[0]);
        result <<= 8;
        result += Long.parseLong(parts[1]);
        result <<= 8;
        result += Long.parseLong(parts[2]);
        result <<= 8;
        result += Long.parseLong(parts[3]);
        return result;
    }

    public static long parseDate(String date) {
        try {
            return new SimpleDateFormat("MM/dd/yyyy HH:mm:ss").parse(date).getTime();
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            return 0;
        }
    }

    @Test
    public void testMatchingCustomAnalyzerQuery() throws IOException {
        HashMap<String, String> args = new HashMap<>();
        args.put("pattern", "[^\\p{L}\\d]+");
        CustomAnalyzer fieldAnalyzer = CustomAnalyzer.builder()
                .withTokenizer("pattern", args)
                .addTokenFilter("lowercase")
                .build();
        CustomAnalyzer queryAnalyzer = CustomAnalyzer.builder()
                .withTokenizer("keyword")
                .addTokenFilter("lowercase")
                .build();
        Monitor monitor = new Monitor(new LuceneQueryParser(defaultField, queryAnalyzer), new TermFilteredPresearcher());
        monitor.update(new MonitorQuery(queryId, nameField + ":FrameWork.log"));
        InputDocument document = InputDocument.builder(docId)
                .addField(nameField, "FrameWork.log", fieldAnalyzer)
                .build();
        Matches<ExplainingMatch> matches = monitor.match(document, ExplainingMatcher.FACTORY);
        Assert.assertTrue(matches.getErrors().isEmpty());
        Assert.assertFalse(matches.getMatches(docId).getMatches().isEmpty());
    }

    @Test
    public void testNonMatchingCustomAnalyzerQuery() throws IOException {
        HashMap<String, String> args = new HashMap<>();
        args.put("pattern", "[^\\p{L}\\d]+");
        CustomAnalyzer fieldAnalyzer = CustomAnalyzer.builder()
                .withTokenizer("pattern", args)
                .addTokenFilter("lowercase")
                .build();
        CustomAnalyzer queryAnalyzer = CustomAnalyzer.builder()
                .withTokenizer("keyword")
                .addTokenFilter("lowercase")
                .build();
        Monitor monitor = new Monitor(new LuceneQueryParser(defaultField, queryAnalyzer), new TermFilteredPresearcher());
        monitor.update(new MonitorQuery(queryId, nameField + ":FrameWork.log"));
        InputDocument document = InputDocument.builder(docId)
                .addField(nameField, "FrameWrk.log", fieldAnalyzer)
                .build();
        Matches<ExplainingMatch> matches = monitor.match(document, ExplainingMatcher.FACTORY);
        Assert.assertTrue(matches.getErrors().isEmpty());
        Assert.assertTrue(matches.getMatches(docId).getMatches().isEmpty());
    }
}
