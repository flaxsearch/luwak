package uk.co.flax.luwak;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import uk.co.flax.luwak.presearcher.TermFilteredPresearcher;
import uk.co.flax.luwak.termextractor.QueryTerm;
import uk.co.flax.luwak.termextractor.querytree.TreeWeightor;
import uk.co.flax.luwak.termextractor.weights.MinWeightCombiner;
import uk.co.flax.luwak.termextractor.weights.TokenLengthNorm;
import uk.co.flax.luwak.termextractor.weights.WeightPolicy;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class TestNestQueryExtraction {
    private static final int TIMES_TO_WARMUP = 300;
    private static final int TIMES_TO_RUN = 300;
    private static final String FIELD = "f";
    private static final int LEVELS_OF_DEPTH_START = 4;
    private static final int LEVELS_OF_DEPTH_END_INCL = 23;
    private static final int BRANCHES_PER_LEVEL = 2;

    public static void main(String... args) throws IOException, UpdateException {
        for (int levelsOfDepth = LEVELS_OF_DEPTH_START; levelsOfDepth <= LEVELS_OF_DEPTH_END_INCL; levelsOfDepth++) {
            Map<Integer, Query> queryPerDepth = Collections.singletonMap(levelsOfDepth, makeLargeQuery(levelsOfDepth));
            System.out.println("-------------");
            long[] benchmarkTimes = new long[TIMES_TO_RUN];
            for (int i = 0; i < TIMES_TO_RUN + TIMES_TO_WARMUP; i++) {
                System.gc();
                long start = System.nanoTime();
                ReportingTokenLengthNorm norm = makeNorm();
                try (Monitor monitor = new Monitor(new GenerateQueryParser(queryPerDepth), new TermFilteredPresearcher(makeTreeWeightor(norm)))) {
                    MonitorQuery mq = new MonitorQuery(Integer.toString(levelsOfDepth), Integer.toString(levelsOfDepth));
                    monitor.update(mq);
                    if (i >= TIMES_TO_WARMUP) {
                        long totalTime = System.nanoTime() - start;
                        benchmarkTimes[i - TIMES_TO_WARMUP] = totalTime;
                    }
                    //if (i == TIMES_TO_WARMUP + TIMES_TO_RUN - 1) {
                        //Final run, let's just print out the map:
                     //   norm.countAskedForMap.entrySet().stream().filter(entry ->entry.getValue() > 1).limit(200).forEach(e ->System.out.println(e.getKey() + " - " + e.getValue()));
                    //}
                }
            }
            System.out.println("Stats for levels of depth - "+levelsOfDepth + "----");
            printStats(benchmarkTimes);
        }
    }

    private static ReportingTokenLengthNorm makeNorm() {
        return new ReportingTokenLengthNorm(3, 0.3f);
    }
    private static TreeWeightor makeTreeWeightor(ReportingTokenLengthNorm tln) {
        return new TreeWeightor(new WeightPolicy.Default(tln),
                new MinWeightCombiner());
    }


    private static void printStats(long[] times) {
        System.out.println(Arrays.stream(times).map(TimeUnit.NANOSECONDS::toMillis).summaryStatistics());
    }


    private static Query makeLargeQuery(int numLevels) {
        return new BooleanQuery.Builder()
                .add(new TermQuery(new Term("otherField", "blahhh")), BooleanClause.Occur.SHOULD)
                .add(new MatchNoDocsQuery(), BooleanClause.Occur.SHOULD)
                .add(makeLargeQuery(numLevels, 0), BooleanClause.Occur.SHOULD)
                .build();
    }


    private static SpanQuery makeLargeQuery(int level, int branchNumber) {
        level--;
        if (level > 0) {
            SpanQuery[] queries = new SpanQuery[BRANCHES_PER_LEVEL];
            for (int i = 0; i < queries.length; i++) {
                queries[i] = makeLargeQuery(level, i);
            }
            if (BRANCHES_PER_LEVEL >= 2) {
                return new SpanNearQuery(queries, 15, false);
            }
            return new SpanOrQuery(queries);
        } else {
            // level == 0, so return a termQuery
            return BASE_FAKE_QUERY;
        }
    }

    private static final BytesRef TOKEN_TEXT = new BytesRef("foobar");
    private static final Term FAKE_TOKEN_TERM = new Term(FIELD, TOKEN_TEXT);
    private static final SpanTermQuery BASE_FAKE_QUERY = new SpanTermQuery(makeFakeTokenTerm());

    private static Term makeFakeTokenTerm() {
        return FAKE_TOKEN_TERM;
    }

    private static final class GenerateQueryParser implements MonitorQueryParser {

        private final Map<Integer, Query> queryMap;

        GenerateQueryParser(Map<Integer, Query> queryMap) {
            this.queryMap = queryMap;
        }

        @Override
        public Query parse(String queryString, Map<String, String> metadata) throws Exception {
            return queryMap.get(Integer.valueOf(queryString));
        }
    }

    private static final class ReportingTokenLengthNorm extends TokenLengthNorm {

        private final Map<Integer, Integer> countAskedForMap = new HashMap<>();

        public ReportingTokenLengthNorm(float a, float k) {
            super(a, k);
        }

        @Override
        public float norm(QueryTerm term) {
            float toReturn = super.norm(term);
            Integer identity = System.identityHashCode(term);
            countAskedForMap.compute(identity, (key, currCount) -> currCount == null ? 1 : currCount + 1);
            return toReturn;
        }

    }
}
