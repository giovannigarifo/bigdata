# Spark Streaming

a framework for large scale stream processing that scales to thousands of nodes, can achieve second scale latencies and proves a simple **batch-like** API for implementing complex algorithms. Can absorb live data streams from Kafka, Twitter, Flume...

It allow to process large stream of live produces data and provide results in **near real-time**. It's useful for social network trends analisys, website statistics, Intrusion Detection Systems...

It's fault tolerant by design, input data are replicated in memory of multiple worker nodes, data lost due to worker failure can be recomputed from input data.


### Discretization of the stream processing

It runs a **streaming computation** as a series of very small and deterministic **batch jobs**: it splits each input stream in "portions" called **batches** and processes one portion at a time, in the incoming order. The same computation is applied to each batch of the stream.

1) input data stream is splitted into batches of "x" seconds
2) each batch is treated as an RDD and is processed using RDD operations
3) finally, the processed results of the RDD operations are returned in batches.


## Key concepts

* **DStream**, is a sequence of RDDs representing a discretized version of the input stream of data. One RDD for each batch of the input stream.

 **PairDStream**, is a sequence of PairRDDs representing a stream of pairs

* **Transformations**, operations that modify data from on DStream to another, includes standard RDD operations and **Window** and **Stateful** operations (window, countByValueAndWindow,...).

* **Actions**, output operationsthat send data to external entity: saveAsHadoopFiles, saveAsTextFile,...


## Basic structure of a Spark Streaming program

```
// Create a configuration object and set the name of the application
SparkConf conf = new SparkConf().setAppName("Spark Streaming word count");

// Create a Spark Streaming Context object
JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(10));

// Create a (Receiver) DStream that will connect to localhost:9999
JavaReceiverInputDStream<String> lines = jsc.socketTextStream("localhost", 9999);


// Apply the "standard" transformations to perform the word count task
// However, the "returned" RDDs are DStream/PairDStream RDDs
JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(line.split("\\s+")).iterator());

JavaPairDStream<String, Integer> wordsOnes = words.mapToPair(word ->
    new Tuple2<String, Integer>(word.toLowerCase(), 1));

JavaPairDStream<String, Integer> wordsCounts = wordsOnes.reduceByKey((i1, i2) -> i1 + i2);

wordsCounts.print();
wordsCounts.dstream().saveAsTextFiles(outputPathPrefix, "");


// Start the computation
jsc.start();
jsc.awaitTerminationOrTimeout(120000);
jsc.close();
```


### Spark Streaming Context

define the size of the batches, in seconds, in his constructor parameters. Specify the input stream and define a DSstream based on it. Specify the operations to be executed for each batch of data.

It also invoke the start method that start the processing of the input stream. 


### Input stream sources

different sources are available, methods of the spark streaming context:

* **TCP socket**: `JavaReceiverInputDStream<String> socketTextStream(String hostname, int port_number)`

* **HDFS folder**: `JavaDStream<String> textFileStream(String folder)` every time a new file is inserted in the folder, the context of the file is stoed in the associated DStream and it's processed. 


## Transformations

Like standard RDDs, also DStream are characterized by a set of transformations: when applied to a DStream object, transformations return a new DStream Object. Each transformation **is applied to one batch** of the input DStream at a time.

* **map(func)**, returns a new DStream by passing each elementof the source DStream through a function func

* **flatMap(func)**, Each input item can be mapped to 0 or more output items. Returns a new DStream.

* **filter(func)**, Returns a new DStream by selecting only the records of the source DStream on which func returns true.

* **reduce(func)**, Returns a new DStream of single-element RDDs by aggregating the elements in each RDD of the source DStream using a function func. The function should be associative so that it can be computed in parallel.

* **reduceByKey(func)**, When called on a PairDStream of (K, V) pairs, returns a new PairDStream of (K, V) pairs where the values for each key are aggregated using the given reduce function

* **countByValue**, When called on a DStream of elements of type K, returns a new PairDStream of (K, Long) pairs where the value of each key is its frequency in each batch of the source DStream

* **count()**, Returns a new DStream of single-element RDDs by counting the number of elements in each batch (RDD) of the source Dstream

* **union(otherDStream)**, Returns a new DStream that contains the union of the elements in the source DStream and otherDStream.

* **join(otherDStream)**, When called on two PairDStreams of (K, V) and (K, W) pairs, return a new PairDStream of (K, (V, W)) pairs with all pairs of elements for each key

* **cogroup(otherDStream)**, When called on a PairDStream of (K, V) and (K, W) pairs, return a new DStream of (K, Seq[V], Seq[W]) tuples


### Advanced transformations for DStreams

* **transform(func)**, it's a specific transformation of DStreams. It returns a new DStream by applying an RDD-to-RDD function to every RDD of the source DStream. This can be used to join every batch in a data stream with another dataset, i.e. a standard RDD.

* **transformToPair(func)**, it returns a new PairDStream by applyind a PairRDD-to-PairRDD function to every PairRDD of the source PairDStream. it must be used instead of transform when working with PairDStreams/PairRDDs.

Code sample:
``` 
// Sort the content/the pairs by key
JavaPairDStream<String, Integer> wordsCountsSortByKey = wordsCounts
    .transformToPair((JavaPairRDD<String, Integer> rdd) -> rdd.sortByKey());
```


### Basic output actions for DStreams

* **print()**, print first q' elements of every batch of data in a DStream on the driver node running the stream application.

* **saveAsTextFiles(prefix, [suffix])**, it's not directly available for JavaDStream object, first a DStream must be created using the `dstream()` method of JavaDStream. One folder for each batch, folder name is "prefix-TIME_IN_MS[.suffix]“. 

```
Counts.dstream().saveAsTextFiles(outputPathPrefix, ""); //Counts is a JavaDStream
```

### Start and run the computation

* **start()** method of the jsc, start the application execution on the input stream.
* **awaitTerminationOrTimeout(long ms)** method is used to specify how long the application will run. If no milliseconds are specified the application will run until is explicitly killed.



# Window operations

Spark also allow to do computations over a sliding window of data: each window contains a set of batches of the input stream, windows can be overlapped, i.e. the same batch can be included in consecutive windows.

Any window operations must specify two parameters, that **must be multiples of the batch interval** of the source DStream:

* **window length**, the duration of the window.
* **sliding interval**, the interval at which the window operation is performed


### Checkpoints

They're needed by the window and state functionality. The checkpoint feature is used ot store temporary data so that the application can be resilient to data lost during it's execution.

## Basic Window transformations

* **window(windowLength, slideInterval)**, returns a new DStream which is computed based on windowed batches of the source DStream.

* **JavaDStream\<Long> countByWindow(windowLength, slideInterval)**, returns a new single-element stream containing the number of elements of each window. The returned DStream contains only one value for each window (the number of elements of the last analyzed window)

* **reduceByWindow(func, windowLength, slideInterval)**, Returns a new single-element stream, created by aggregating elements in the stream over a sliding interval using func. The function should be associative so that it can be computed correctly in parallel

* **countByValueAndWindow(windowLength, slideInterval)** When it is called on a PairDStream of (K, V) pairs, returns a new PairDStream of (K, Long) pairs where the value of each key is its frequency within a sliding window

* **reduceByKeyAndWindow(func, windowLength, slideInterval)** When called on a PairDStream of (K, V) pairs, returns a new PairDStream of (K, V) pairs where the values for each key are aggregated using the given reduce function over batches in a sliding window. The window length and the sliding window step are specified as parameters of this invokation


Code sample, word count with windows:

```
String outputPathPrefix = args[0];

// Create a configuration object and set the name of the application
SparkConf conf = new SparkConf().setAppName("Spark Streaming word count");

// Create a Spark Streaming Context object
JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));

// Set the checkpoint folder (it is needed by some window transformations)
jssc.checkpoint("checkpointfolder");

// Create a (Receiver) DStream that will connect to localhost:9999
JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);

// Apply the "standard" trasformations to perform the word count task
// However, the "returned" RDDs are DStream/PairDStream RDDs

JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(line.split("\\s+")).iterator());

// Count the occurrence of each word in the current window
JavaPairDStream<String, Integer> wordsOnes = words
    .mapToPair(word -> new Tuple2<String, Integer>(word.toLowerCase(), 1));

// reduceByKeyAndWindow is used instead of reduceByKey
// The characteristics of the window is also specified

JavaPairDStream<String, Integer> wordsCounts = wordsOnes
    .reduceByKeyAndWindow( (i1, i2) -> i1 + i2, Durations.seconds(30), Durations.seconds(10));

// Print the num. of occurrences of each word of the current window (only 10 of them)
wordsCounts.print();

// Store the output of the computation in the folders with prefix outputPathPrefix
wordsCounts.dstream().saveAsTextFiles(outputPathPrefix, "");

// Start the computation
jssc.start();
jssc.awaitTerminationOrTimeout(120000);
jssc.close();
```


### Maintaining a state

the **updateStateByKey** transformation allows to maintain a state. The value of the state is continuously updated every time a new batch is analyzed. It's based on two steps:

1) Define the state, the data type of the state can be arbitrary.
2) Define the state update function, it specify how to update the previous one and the new values from an input stream.

In every batch, Spark will apply the state update function for all existing keys. For each each key, the update function is used to update the value associated with a key by combining the former value and the new values associated with that key. For each key, the call method of the “function” is invoked on the list of new values and the former state value and returns the new aggregated value for the considered key.

Code sample: word count stateful version

By using the UpdateStateByKey the application can continuously update the number of occurrences of each word.

```
// Create a configuration object and set the name of the application
SparkConf conf = new SparkConf().setAppName("Spark Streaming word count");

// Create a Spark Streaming Context object
JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));

// Set the checkpoint folder (it is needed by some window transformations)
jssc.checkpoint("checkpointfolder");

// Create a (Receiver) DStream that will connect to localhost:9999
JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);

// Apply the "standard" transformations to perform the word count task
// However, the "returned" RDDs are DStream/PairDStream RDDs
JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(line.split("\\s+")).iterator());

JavaPairDStream<String, Integer> wordsOnes = words
    .mapToPair(word -> new Tuple2<String, Integer>(word.toLowerCase(), 1));

// DStream made of get cumulative counts that get updated in every batch.
// the function is called one time for each key
JavaPairDStream<String, Integer> totalWordsCounts = wordsCounts
    .updateStateByKey((newValues, state) -> {
        // state.or(0) returns the value of State
        // or the default value 0 if state is not defined
        Integer newSum = state.or(0);
        // Iterates over the values of the current key and sum them to the previous state
        for (Integer value : newValues) {
            newSum += value;
        }
        return Optional.of(newSum);
    });

```