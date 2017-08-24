# What is it?
hh-metrics is a java library that aggregates metrics in memory
and sends aggregated values to a monitoring system using statsd protocol.

# Motivation
## In memory aggregation
In case of thousands events per second you do not want to send each event over network because of serialization and network overhead
as well as high probability of loosing events when using UDP.
It is better to aggregate a stream of events in memory and periodically send aggregated values to a monitoring system.

## Metric breakdown
In addition, we find it convenient to track events with breakdown by tags.
For example "number of cache calls with breakdown by regions and nodes".
Then you can answer questions like: What is a total number of cache calls? What are most popular cache regions? What cache nodes are down and do not reply?

## Memory limits
There is a risk of consuming too much memory if a tag has too many values.
For example, you decide to track "how much database time is consumed by each url of your application".
If you forget to remove ids from urls, you'll end up with thousands of independent counters that can consume too much memory.

Another risk is using many tags.
For example, if a metric is broken down by 3 tags: tag1 has 10 distinct values, tag2 has 100 distinct values and tag3 has 1000 distinct values,
you will end up with 10 * 100 * 1000 = 1 000 000 different combinations that again can consume too much memory.

To prevent high memory consumption we use several techniques.
1) Our aggregators has a mandatory limit on the maximum number of distinct tags.
2) Each time we get a snapshot of aggregated values to be sent to a monitoring system, we reset an aggregator.

## Why not Dropwizard Metrics?
At the time of writing Dropwizard Metrics library does not have metric breakdowns, memory limits and native support of statsd. 

# Constraints
We use popular StatsD protocol to send metrics to a monitoring system.
Unfortunately, statsd does not have such concept as "metric breakdown".
You should somehow encode tags in a metric name. We do it like this:
```
cache.calls.region_is_vacancy.node_is_127-0-0-1
```
In this case metric name is `cache.calls`, cache region is `vacancy` and cache node is `127-0-0-1`.

This scheme should be supported by your monitoring system.
See, for example, [okmeter.io](https://okmeter.io/).

# Build
We use Apache Maven so just:
```bash
mvn install
```

# Quick guide
Pretend you have thousands calls to a cache and you want to send number of cache hits to your monitoring system.

But just a number of hits is not very informative. You want to have cache hits broken down by cache regions and nodes.

Here is an example of how it can be accomplished:
```java
public class CacheClient {
  
  private final Counters hitCounters;
  
  public CacheClient(StatsDSender statsDSender) {
    // Create Counters aggregator that maintains a separate counter for each combination of tags (region and node in our case).
    int maxNumOfCounters = 500;
    this.hitCounters = new Counters(maxNumOfCounters);
    
    // Register hitCounters in statsDSender to periodically send accumulated values to a monitoring system
    String hitCountersMetricName = "cache.hits";
    statsDSender.sendCountersPeriodically(hitCountersMetricName, hitCounters);
  }
  
  public Object get(String region, int id) {
    // Some code here that fetches an object from a cache node by region and id
    // ...
    // Register hit
    hitCounters.add(1, new Tag("region", region), new Tag("node", node));
  }
}

```
Here we use [Counters](src/main/java/ru/hh/metrics/Counters.java) aggregator.
There are also other types of aggregators: [Histogram](src/main/java/ru/hh/metrics/Histogram.java),
[Histograms](src/main/java/ru/hh/metrics/Histograms.java),
[Max](src/main/java/ru/hh/metrics/Max.java).
See java doc for more details.

When you register hitCounters in [StatsDSender](src/main/java/ru/hh/metrics/StatsDSender.java), it periodically gets snapshot of hitCounters, sends accumulated values to statsd server
and resets hitCounters to prevent overflow and reduce memory consumption.
  
[StatsDSender](src/main/java/ru/hh/metrics/StatsDSender.java) can be reused between different parts of your application and is passed to the CacheClient as a dependency.
It can be created like this:
```java
String statsdHost = "localhost";
int statsdPort = 8125;
int maxQueueSize = 10_000;
String prefix = null;
StatsDClient statsDClient = new NonBlockingStatsDClient(prefix, statsdHost, statsdPort, maxQueueSize);

StatsDSender statsDSender = new StatsDSender(statsDClient, scheduledExecutorService);
```
We use [java-dogstatsd-client](https://github.com/DataDog/java-dogstatsd-client) as StatsDClient.
