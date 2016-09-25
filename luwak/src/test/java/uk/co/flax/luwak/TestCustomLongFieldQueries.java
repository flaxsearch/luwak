package uk.co.flax.luwak;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.junit.Assert;
import org.junit.Test;
import uk.co.flax.luwak.matchers.ExplainingMatch;
import uk.co.flax.luwak.matchers.ExplainingMatcher;
import uk.co.flax.luwak.presearcher.TermFilteredPresearcher;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Map;

/**
 * Created by edvorg on 16/09/25.
 * date field query in luwak/lucene
 * ip field query in luwak/lucene
 */
public class TestCustomLongFieldQueries {
    private static final String defaultField = "field";
    private static final String ipField = "ip";
    private static final String dateField = "date";
    private static final String docId = "doc-id";
    private static final String queryId = "query-id";

    private static long parseIp(String ip) {
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

    private static long parseDate(String date) {
        try {
            return new SimpleDateFormat("MM/dd/yyyy HH:mm:ss", Locale.ENGLISH).parse(date).getTime();
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            return 0;
        }
    }

    @Test
    public void testNonMatchingIpLongRangeQuery() throws IOException {
        Monitor monitor = new Monitor(new CustomLongFieldRangeQueryParser(), new TermFilteredPresearcher());
        monitor.update(new MonitorQuery(queryId, ipField + ":[\"192.168.2.1\" TO \"192.168.3.1\"]"));
        InputDocument document = InputDocument.builder(docId)
            .addField(new LongPoint(ipField, parseIp("192.168.1.1")))
            .build();
        Matches<ExplainingMatch> matches = monitor.match(document, ExplainingMatcher.FACTORY);
        Assert.assertTrue(matches.getErrors().isEmpty());
        Assert.assertTrue(matches.getMatches(docId).getMatches().isEmpty());
    }

    @Test
    public void testMatchingIpLongRangeQuery() throws IOException {
        Monitor monitor = new Monitor(new CustomLongFieldRangeQueryParser(), new TermFilteredPresearcher());
        monitor.update(new MonitorQuery(queryId, ipField + ":[\"192.168.0.1\" TO \"192.168.2.1\"]"));
        InputDocument document = InputDocument.builder(docId)
            .addField(new LongPoint(ipField, parseIp("192.168.1.1")))
            .build();
        Matches<ExplainingMatch> matches = monitor.match(document, ExplainingMatcher.FACTORY);
        Assert.assertTrue(matches.getErrors().isEmpty());
        Assert.assertFalse(matches.getMatches(docId).getMatches().isEmpty());
    }

    @Test
    public void testNonMatchingIpLongQuery() throws IOException {
        Monitor monitor = new Monitor(new CustomLongFieldRangeQueryParser(), new TermFilteredPresearcher());
        monitor.update(new MonitorQuery(queryId, ipField + ":\"192.168.0.1\""));
        InputDocument document = InputDocument.builder(docId)
            .addField(new LongPoint(ipField, parseIp("192.168.1.1")))
            .build();
        Matches<ExplainingMatch> matches = monitor.match(document, ExplainingMatcher.FACTORY);
        Assert.assertTrue(matches.getErrors().isEmpty());
        Assert.assertTrue(matches.getMatches(docId).getMatches().isEmpty());
    }

    @Test
    public void testMatchingIpLongQuery() throws IOException {
        Monitor monitor = new Monitor(new CustomLongFieldRangeQueryParser(), new TermFilteredPresearcher());
        monitor.update(new MonitorQuery(queryId, ipField + ":\"192.168.1.1\""));
        InputDocument document = InputDocument.builder(docId)
            .addField(new LongPoint(ipField, parseIp("192.168.1.1")))
            .build();
        Matches<ExplainingMatch> matches = monitor.match(document, ExplainingMatcher.FACTORY);
        Assert.assertTrue(matches.getErrors().isEmpty());
        Assert.assertFalse(matches.getMatches(docId).getMatches().isEmpty());
    }

    @Test
    public void testNonMatchingDateLongRangeQuery() throws IOException {
        Monitor monitor = new Monitor(new CustomLongFieldRangeQueryParser(), new TermFilteredPresearcher());
        monitor.update(new MonitorQuery(queryId, dateField + ":[\"09/06/2008 13:05:14\" TO \"09/06/2008 13:06:14\"]"));
        InputDocument document = InputDocument.builder(docId)
            .addField(new LongPoint(dateField, parseDate("09/06/2008 13:04:14")))
            .build();
        Matches<ExplainingMatch> matches = monitor.match(document, ExplainingMatcher.FACTORY);
        Assert.assertTrue(matches.getErrors().isEmpty());
        Assert.assertTrue(matches.getMatches(docId).getMatches().isEmpty());
    }

    @Test
    public void testMatchingDateLongRangeQuery() throws IOException {
        Monitor monitor = new Monitor(new CustomLongFieldRangeQueryParser(), new TermFilteredPresearcher());
        monitor.update(new MonitorQuery(queryId, dateField + ":[\"09/06/2008 13:03:14\" TO \"09/06/2008 13:05:14\"]"));
        InputDocument document = InputDocument.builder(docId)
            .addField(new LongPoint(dateField, parseDate("09/06/2008 13:04:14")))
            .build();
        Matches<ExplainingMatch> matches = monitor.match(document, ExplainingMatcher.FACTORY);
        Assert.assertTrue(matches.getErrors().isEmpty());
        Assert.assertFalse(matches.getMatches(docId).getMatches().isEmpty());
    }

    @Test
    public void testNonMatchingDateLongQuery() throws IOException {
        Monitor monitor = new Monitor(new CustomLongFieldRangeQueryParser(), new TermFilteredPresearcher());
        monitor.update(new MonitorQuery(queryId, dateField + ":\"09/06/2008 13:05:14\""));
        InputDocument document = InputDocument.builder(docId)
            .addField(new LongPoint(dateField, parseDate("09/06/2008 13:04:14")))
            .build();
        Matches<ExplainingMatch> matches = monitor.match(document, ExplainingMatcher.FACTORY);
        Assert.assertTrue(matches.getErrors().isEmpty());
        Assert.assertTrue(matches.getMatches(docId).getMatches().isEmpty());
    }

    @Test
    public void testMatchingDateLongQuery() throws IOException {
        Monitor monitor = new Monitor(new CustomLongFieldRangeQueryParser(), new TermFilteredPresearcher());
        monitor.update(new MonitorQuery(queryId, dateField + ":\"09/06/2008 13:04:14\""));
        InputDocument document = InputDocument.builder(docId)
            .addField(new LongPoint(dateField, parseDate("09/06/2008 13:04:14")))
            .build();
        Matches<ExplainingMatch> matches = monitor.match(document, ExplainingMatcher.FACTORY);
        Assert.assertTrue(matches.getErrors().isEmpty());
        Assert.assertFalse(matches.getMatches(docId).getMatches().isEmpty());
    }

    private class CustomLongFieldRangeQueryParser implements MonitorQueryParser {
        @Override
        public Query parse(final String queryString, Map<String, String> metadata) throws Exception {
            return new QueryParser(defaultField, new StandardAnalyzer()) {
                @Override
                protected Query getRangeQuery(String field, String part1, String part2, boolean startInclusive, boolean endInclusive) throws ParseException {
                    if (ipField.equals(field)) {
                        long ip1 = parseIp(part1);
                        long ip2 = parseIp(part2);
                        return LongPoint.newRangeQuery(field, ip1, ip2);
                    } else if (dateField.equals(field)) {
                        long date1 = parseDate(part1);
                        long date2 = parseDate(part2);
                        return LongPoint.newRangeQuery(field, date1, date2);
                    }
                    return super.getRangeQuery(field, part1, part2, startInclusive, endInclusive);
                }

                @Override
                protected Query newTermQuery(Term term) {
                    if (ipField.equals(term.field())) {
                        return LongPoint.newExactQuery(ipField, parseIp(term.text()));
                    }
                    return super.newTermQuery(term);
                }

                @Override
                protected Query getFieldQuery(String field, String queryText, boolean quoted) throws ParseException {
                    if (dateField.equals(field)) {
                        return LongPoint.newExactQuery(dateField, parseDate(queryText));
                    }
                    return super.getFieldQuery(field, queryText, quoted);
                }

            }.parse(queryString);
        }
    }
}
