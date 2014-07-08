package uk.co.flax.luwak;

public interface MonitorQueryCollectorFactory {
    public MonitorQueryCollector get(QueryCache queryCache, CandidateMatcher matcher);
}
