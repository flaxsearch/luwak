package uk.co.flax.luwak.matchers;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.Test;
import uk.co.flax.luwak.*;
import uk.co.flax.luwak.presearcher.MatchAllPresearcher;
import uk.co.flax.luwak.queryparsers.LuceneQueryParser;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

public class TestSimpleMatcher {

    @Test
    public void testSimpleMatcher() throws IOException, UpdateException {

        try (Monitor monitor = new Monitor(new LuceneQueryParser("field"), new MatchAllPresearcher())) {
            monitor.update(new MonitorQuery("1", "test"), new MonitorQuery("2", "wibble"));

            InputDocument doc1 = InputDocument.builder("doc1").addField("field", "test", new StandardAnalyzer()).build();

            Matches<QueryMatch> matches = monitor.match(doc1, SimpleMatcher.FACTORY);
            assertThat(matches.matches("1", "doc1")).isNotNull();
        }
    }
}
