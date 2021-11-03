Luwak - stored query engine from Flax                   
=====================================
[![Build
Status](https://travis-ci.org/flaxsearch/luwak.svg?branch=master)](https://travis-ci.org/flaxsearch/luwak)

*Luwak is now part of the Apache Lucene library as of the v8.2.0 release - see the details [here](https://lucene.apache.org/core/8_2_0/changes/Changes.html#v8.2.0.new_features).*
=================================================================================================

What is Luwak?
--------------

Based on the open source Lucene search library, Luwak is a high performance stored query engine. Simply put, it allows you to define a set of search queries and then monitor a stream of documents for any that might match these queries: a function also known as 'reverse search' and 'document routing'. Flax developed Luwak for clients who monitor high volumes of news using often extremely complex Boolean expressions. Luwak is being used by companies including Infomedia, Bloomberg http://www.flax.co.uk/blog/2016/03/08/helping-bloomberg-build-real-time-news-search-engine/ and Booz Allen Hamilton.

You can find out a bit more about how Flax use Luwak for media monitoring applications in 
this video from Lucene Revolution 2013 http://www.youtube.com/watch?v=rmRCsrJp2A8 and this video
from Berlin Buzzwords 2014 https://www.youtube.com/watch?v=M3c5_7DxpIk and how we combined it with Apache Samza (including a great illustration of how Luwak internals work) http://www.flax.co.uk/blog/2015/08/26/real-time-full-text-search-with-luwak-and-samza/

Here's some tests we did to compare Luwak to Elasticsearch Percolator:
http://www.flax.co.uk/blog/2015/07/27/a-performance-comparison-of-streamed-search-implementations/

Scott Stults of Open Source Connections wrote "How to use Luwak to run preset queries against incoming documents":
http://opensourceconnections.com/blog/2016/02/05/luwak and Ryan Walker wrote a complete streaming search implementation with Luwak http://insightdataengineering.com/blog/streaming-search/

Get the artifacts
-----------------

```
<dependency>
  <groupId>com.github.flaxsearch</groupId>
  <artifactId>luwak</artifactId>
  <version>1.4.0</version>
</dependency>
```

Using the monitor
-----------------

Basic usage looks like this:

```java
Monitor monitor = new Monitor(new LuceneQueryParser("field"), new TermFilteredPresearcher());

MonitorQuery mq = new MonitorQuery("query1", "field:text");
List<QueryError> errors = monitor.update(mq);

// match one document at a time
InputDocument doc = InputDocument.builder("doc1")
                        .addField(textfield, document, new StandardAnalyzer())
                        .build();
Matches<QueryMatch> matches = monitor.match(doc, SimpleMatcher.FACTORY);

// or match a batch of documents
Matches<QueryMatch> matches = monitor.match(DocumentBatch.of(listOfDocuments), SimpleMatcher.FACTORY);
```

Adding queries
--------------

The monitor is updated using MonitorQuery objects, which consist of an id, a query string, and an
optional metadata map.  The monitor uses its provided MonitorQueryParser
to parse the query strings and cache query objects.

In Luwak 1.5.0, errors thrown when adding queries (from query parsing, for example) cause an
UpdateException to be thrown, detailing which queries could not be added.

In Luwak 1.4 and below, ```Monitor.update()``` returns a list of ```QueryError``` objects, which should 
be checked for parse errors.  The list will be empty if all queries were added successfully.

Matching documents
------------------

Queries selected by the monitor to run against an InputDocument are passed to a CandidateMatcher
class.  Four basic implementations are provided:
* SimpleMatcher - reports which queries matched the InputDocument
* ScoringMatcher - reports which queries matched, with their scores
* ExplainingMatcher - reports which queries matched, with an explanation for their scores
* HighlightingMatcher - reports which queries matched, with the individual matches for each query

In addition, luwak has two multithreaded matchers which wrap the simpler matchers:
* ParallelMatcher - runs queries in multiple threads as they are collected from the Monitor
* PartioningMatcher - collects queries, partitions them into groups, and then runs each group in its own thread

Running the demo
----------------

A small demo program is included in the distribution that will run queries provided
in a text file over a small corpus of documents from project gutenberg (via nltk).

```sh
./run-demo
```

Filtering out queries
---------------------

The monitor uses a ```Presearcher``` implementation to reduce the number of queries it runs
during a ```match``` run.  Luwak comes with three presearcher implementations.

### MatchAllPresearcher

This Presearcher does no filtering whatsoever, so the monitor will run all its registered
queries against every document passed to ```match```.

### TermFilteredPresearcher

This Presearcher extracts terms from each registered query and indexes the queries against them
in the Monitor's internal index.  At match-time, the passed-in ```InputDocument``` is tokenized
and converted to a disjunction query.  All queries that match this query in the monitor's index
are then run against the document.

### MultipassTermFilteredPresearcher

An extension of ```TermFilteredPresearcher``` that tries to improve filtering on phrase queries
by indexing different combinations of terms.

The TermFilteredPresearcher can be configured with different ```PresearcherComponent```
implementations - for example, you can ignore certain fields with a ```FieldFilterPresearcherComponent```,
or get accurate filtering on wildcard queries with an ```WildcardNGramPresearcherComponent```.

Adding new query types
----------------------

```TermFilteredPresearcher``` extracts terms from queries by using a ```QueryAnalyzer``` to build
a tree representation of the query, and then selecting the best possible set of terms from that tree
that uniquely identify the query.  The tree is built using a set of specialized ```QueryTreeBuilder```
implementations, one for each lucene ```Query``` class.

This will not be appropriate for all custom Query types.  You can create your own custom tree builder by
subclassing ```QueryTreeBuilder```, and then pass it to the ```TermFilteredPresearcher``` in
a ```PresearcherComponent```.

```java
public class CustomQueryTreeBuilder extends QueryTreeBuilder<CustomQuery> {

    public CustomQueryTreeBuilder() {
        super(CustomQuery.class);
    }

    @Override
    public QueryTree buildTree(QueryAnalyzer builder, CustomQuery query) {
        return new TermNode(getYourTermFromCustomQuery(query));
    }

}

...

Presearcher presearcher = new TermFilteredPresearcher(new PresearcherComponent(new CustomerQueryTreeBuilder()));
```

Customizing the existing presearchers
-------------------------------------

Not all terms extracted from a query need to be indexed, and the fewer terms indexed, the
more performant the presearcher filter will be.  For example, a BooleanQuery with many SHOULD
clauses but only a single MUST clause only needs to index the terms extracted from the MUST
clause.  Terms in a parsed query tree are given weights and the ```QueryAnalyzer``` uses these
weights to decide which terms to extract and index.  The weighting is done by a ```TreeWeightor```.

Weighting is configured by a ```WeightPolicy```, which will contain a set of ```WeightNorm```s and
a ```CombinePolicy```.  A query term will be run through all the ```WeightNorm``` objects to determine
its overall weighting, and a parent query will then calculate its weight with the ```CombinePolicy```,
passing in all child weights.

The following ```WeightNorm``` implementations are provided:
* FieldWeightNorm - weight all terms in a given field
* FieldSpecificTermWeightNorm - weight specific terms in specific fields
* TermTypeNorm - weight terms according to their type (EXACT terms, ANY terms, CUSTOM terms)
* TermWeightNorm - weight a specific set of terms with a given value
* TokenLengthNorm - weight a term according to its length
* TermFrequencyWeightNorm - weight a term by its term frequency

A single ```CombinePolicy``` is provided:
* MinWeightCombiner - a parent node's weight is set to the minimum weight of its children

You can create your own rules, or combine existing ones

```java
WeightPolicy weightPolicy = WeightPolicy.Default(new FieldWeightNorm("category", 0.01f));
CombinePolicy combinePolicy = new MinWeightCombiner();

TreeWeightor weightor = new TreeWeightor(weightPolicy, combinePolicy);
Presearcher presearcher = new TermFilteredPresearcher(weightor);
```

You can debug the output of any weightor by using a ```ReportingWeightor```.  ```QueryTreeViewer```
is a convenience class that may help here.

Creating an entirely new type of Presearcher
--------------------------------------------

You can implement your own query filtering code by subclassing ```Presearcher```.  You will need
to implement ```buildQuery(InputDocument, QueryTermFilter)``` which converts incoming documents into queries to
be run against the Monitor's query index, and ```indexQuery(Query, Map<String,String>)``` which converts registered
queries into a form that can be indexed.

Note that ```indexQuery(Query, Map<String,String>)``` may not create fields named '_id' or '_query', as these are reserved
by the Monitor's internal index.
