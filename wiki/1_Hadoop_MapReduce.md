# Hadoop and MapReduce

Key points:

* **Data locality:** better to move code, not to move data. Data are in the TB order of magnitude, programs are in the order of hundres of KB.

* **Scale up horizontally:** just add more servers. Costs scale up linearly. Need a framework that can be used to abstract the complexiness of the hardware architecture: MapReduce.

* **Process data sequentially:** random access is useless if we want to analyze big files.

* **Doesn't feet well for: **
1) iterative problems
 2) recursive problems
 3) stream data processing
 
 * **Based on the functional programming paradigm:** everything is a function, there are a limited amount of functions and interfaces that do all the job.
 

### Core Components of the Hadoop Architecture

##### Distributed big data processing instrastructure:
based on the MapReduce paradigm that abstract away the distributed part of the problem (scheduling, synchronization, etc).


##### Hadoop Distributed File System:
Abstracts the union of the cluster storage as a single FS. Based on redundancy, provides persistance and availability by being fault-tolerant. Optimized for sequential read/write of huge files in the order of hundreds of megabytes.

Basic architecture:

* the block size of HDFS is of 64 to 128 MB, instead of e.g. linux where is 4KB. Each file is splitted in chunks that are the same size of the FS blocks. So if the file is small there's a lot of wasted space, but in this way operations on very large files (big data sized files) are optimized. Hadoop is fine tuned to be efficient only if it handles very big data sets.
* each **chunk are replicated on 3 different servers**.
* Master node is a special node in the cluster that handles the coordination and stores all the HDFS metadata, such as mapping between filenames and location in the FS.


##### Measures

we define scalability among two dimensions:

* data: if the data increase, how the execution time grows? 
* resources: if the number of nodes increase, how the execution grows?
* network: how much data must be sent over the network? sometimes neglible, it depents on the mapreduce algorithm. The link speed is 1Gbps.



### MapReduce application design

1) define the key-value structure of the input and output data sets
2) write a map function that transforms a key-value pars in a list of key-value pair
3) write a reduce function that reduce a key-ListOfValues pair in a key-value pair

A MapReduce application consists of three main parts implemented by a specific Java class:

* **Driver**, coordinates the configuration of the Hadoop Job and the workflow of the application. Implements the main() method. Runs on the client machine. The developer implements the main() and run() methods

* **Mapper**, a class that implements the map function. Runs on the cluster. 

* **Combiner**, is locally called on the output of the mapper, can be used to do a local aggregation on the key-value pairs that occur multiple times. So it's like a Reducer that is executed on the local result of the Mapper, before sending the final result over the network to the Reducer(s). In fact, is an instance of the Reducer class, locally instantiated in each mapper.

* **Reducer**, a class that implements the reduce function. Runs on the cluster. Receives for each key the list of values associated. **The reducer can iterate among the collection of values JUST ONE TIME.**


The key-value pairs must be serializable because the mappers output is sended over the network to the reducers. Some primitive types are already implemented by Hadoop as serializable objects: `org.apache.hadoop.io.Text` is a serializable java String, same for ....IntWritable, LongWritable, NullWritable ecc.

New serializable data types can be defined by implementing the Writable and/or the WritableComparable interfaces that are present in the org.apache.hadoop.io package.

The input file is splitted by the **InputFormat** class implementation (it's an abstract class). It reads the input file, split it into logical Input Splits that is then assigned to each Mapper. It also provides the RecordReader implementation that can be used to divide the logical Input Splits into key-value pairs. Each Mapper than process a record/key-value at a time. 
Some already implemented InputFormat class: **TextInputFormat, KeyValueInputFormat, SequenceFileInputFormat** ecc..

The OutputFormat class provides the methods to write the final mapreduce program results to the HDFS.


##### Terminology:

* Hadoop Job: execution of a mapreduce code over a data set
* Task: eceution of a mapper or a reducer in a chunk of data, many tasks for a single job.
* Input split: fixed-size piece of the input data, usually the same size of a HDFS block (chunk)


# Hadoop Internals: How it works

#### Tasks error handling

In case of a task failure, hadoop will try to re-execute it again, the maximum number of attempts is a parameter of the cluster configuration. Multiple attempts may occur in parallel if the resources are available, in order to minimize the probability to have a failure.

* Heartbeat based mechanism: the TaskTracker will send periodically a heartbeat message to the JobTracker, if it doesn't respond, it'll be killed.

#### Mappers and Reducer(s)

The emitted data of the mappers are sent over the cluster network using HTTP to the reducers. This is a big bottleneck: avoid to send too much data, this can be done by using **Combiners**




# Develop application for Hadoop: MapReduce Framework


#### Personalized Data Types

New data types defined by a custom class must implements the `org.apache.hadoop.io.Writable` interface, overriding the two methods:

* write, that is used to serialize the fields of the object in a DataOutput object

* readFields, that is used to deserialize the field of the DataInput object in the values of the custom class

* toString, must be overridden because is used to store the result in the output file(s)


#### Sharing Parameters between mappers and reducers

We use the configuration object, it stores a list of (property-name , property values) pairs. The properties must be setted in the Driver, mappers and reducers can only read them, this is to avoid concurrency problems, because there's a single instance of the configuration object:

```
Configuration conf = this.getConf();

conf.set("my-property-name", "value");
```

then, in the mapper and/or reducer:

```
context.getConfiguration().get("my-property-name");
```


#### Counters

Hadoop provides a set of basic built in counters that can be used to store some statistics about jobs, mappers and reducers, e.g.: number of input and output records, number of transmitted bytes. Ad hoc user defined counters can be defined to computer global statistics associated with the application goal.

A counter is an attribute of the Driver, each mapper/driver can only increment/decrement, then the Driver is responbile for aggregate the values of all the local counters incremente by each driver/mapper.

Are defined by means of a Java enumeration.

* the inital value is zero by default, it cannot be changed.

* can be incremented by: `context.getCounter(countername).increment(value)`

* the output value is printed at the end 

at the end of the job, the driver can retrieve the final value.


#### User defined counters

in the driver: 

```
public static enum COUNTERS {
	ERROR_COUNT
}
```

then in the mapper or the reducer:

```
context.getCounter(COUNTERS.ERROR_COUNT).increment(1);
```

then to retrieve the final value **after the job completion**:

``` 
job.getCounters().findCounter(COUNTERS.ERROR_COUNT);
```

#### Map-only jobs

just set the number of reducers to 0.


#### In-Mapper combiner

Mapper classes can also overrides the setup() and cleanup() methods.

* **setup()** method is called one time for each mapper, before the execution of the map() methods. 
* **cleanup()** method is called once for each mappper, after the many calls to the map() methods.

Those methods can be used to implement an In-Mapper combiners, a more efficient approach then classic combiners: 

1) in the setup method instantiate a Collection, define the collection in the Mapper class.
2) in the map method update the collection
3) in the cleanup method emit the final results in the collection and flush the collection.

Please note that this involves the use of main memory to store the Collection data, so it can't be used if we don't now the cardinality of the data to be stored, or the cardinality is too high (the order of available system emmory for each mapper is 1-8GB)



# Handling files with different syntaxes

Semantic content of a data files can be the same, but used syntaxes can be different. 

## Handling multiple inputs
Hadoop permit to specify a different Mapper for each input data file, it's obvious that the Mappers must emit the same key-value types.

in the Driver: use the addInputPath method of the **MultipleInputs** class multiple times to add one input path at a time, specify the input format class, and specify the Mapper class associated with the specified input path.

```
// Set two input paths and two mapper classes
MultipleInputs.addInputPath(job, inputPath1, TextInputFormat.class, MapperType1BigData.class);
MultipleInputs.addInputPath(job, inputPath2, TextInputFormat.class, MapperType2BigData.class);
```

A disambiguation value can be appended in the value of the emitted key-value pairs in the two mappers so that the reducer can behave properly. 


## Handling multiple outputs
It's possible to write the output of a MapReduce job into different files, by means of the methods of the MultipleOutputs class:

* MultipleOutputs.addNamedOutput() to specify the prefixes of the output files

This can be useful for application based on the filtering pattern, so that we can store on different files the result of the filtering.

N.B.: to write it's used a specific method of MultipleOutputs object that accept the prefix of the file in which the output must be stored.



# Distributed cache

Can be used to share and **cache small read-only files** efficiently. Can be used to cache common configuration files, e.g. a list of stopwords.

The cache file is specified in the Driver with job.addCacheFile() method.

The effiency of this solution depends on the number of mappers instantiated for each node: increase if there are multiple mappers for each node because the cache file is sented to each node, so is serverd to multiple mappers.


# MapReduce Patterns

Solutions to common problems that can be solved by using MapReduce.

### Numerical summarizations (statistics)

Used to calculate basic statistics as min, max, avg. Conceptually similar to a SQL group by.


### Inverted index

From a list of documents, can be used to obtain a list of key=word and value=concatenation_of_documents_names in which the word appears


### Counting with Counters

Can be achieved by implementing a map-only job with a counter.


### Filtering Patterns

Used to filter the input records if they match a specific filtering pattern, can be easily implemented with a map-only job. Can be used for data cleaning, distributed grep, record filtering.


### Top K

Select a subset of the records, based on a ranking function. In the mapping phase we can only emit the key-value pairs. In the reducer then we can calculate the top-k for each key. This means that we are sending over the network the whole dataset!. A solution to this problem is to calculate the top-k also for each mapper in a Combiner, so that only top-k records are emitted over the network, then the reducer will compute the global top-k for each key.

In classical top K problems, where we want to perform the **ranking for the whole values, not for each key**, we must use only 1 reducer so that we can perform the ranking on the whole resulting dataset.


### Distinct

delete duplicates from the dataset. Simply emit in the mapper the whole record as key, and NullWritable as value. The reducer will receive for each distinct key a list of null, so simply emit the key.



## Data organization Patterns

### Binning

It's used to split data in different partitions. Based on a map only job: analyze the record and store it in a different "bin", e.g. a prefix using multiple output files.

### Shuffling

Used to randomize. in the mapper we emit (random key, input record), in the reducer we emit (input record, null). So result is as the input file but with shuffled order,from a privacy point of view can be used to hide users behaviours.


### Job Chaining 

the output of a job is the input of another one.


### Join Patterns

Used to implement join operators of the relational algebra. We'll focus on the natural join:

* Reduce side natural join

pseudo-code:

```
map (K table, V rec) {
 
   dept_id = rec.Dept_Id //join attribute
   tagged_rec.tag = table //in the reducer we need to know from which table the record derives
   tagged_rec.rec = rec //the whole record is sended to the reducer
   emit(dept_id, tagged_rec)
}

reduce (K dept_id, list<tagged_rec> tagged_recs)  {
 
   for (tagged_rec : tagged_recs) {
     	for (tagged_rec1 : taagged_recs) {
 
          if (tagged_rec.tag != tagged_rec1.tag) {
        	joined_rec = join(tagged_rec, tagged_rec1)
       		emit (tagged_rec.rec.Dept_Id, joined_rec)
          }
    	}
   }
}
```

other type of joins (e.g. theta join) can be achieved by changing the behaviour of the reducer.



### SQL like operations

* **select**

just perform a map only job emitting only the records that matched the constraint

* **projection**

mapper: emit as key a new record composed only of the values to be retained, and null as value. 

reducer: just emit the key and again null for the list of null values, this allow to remove duplicates.

* **union**

mapper: for each record emit one key-value with key equal to the whole record and null as value

reducer: emit one key value pair for each input key-listOfNull.


* **intersection**

mapper: for each input record, emit one key value pair with key equal to the whole record and value equal to 1.

reducers: emit one key-value pair with key equal to the whole record and value equal to null only if sum of the input values is 2.

* **difference**

mapper: for each input record in the file R emit one key value pair with key equal to the whole record and values equal to the name of the file (R). If the record is from the file S, do the same with S as value.

reducer: emit a pair with key= whole record and value = null for each input pair with value equal to R.


* natural join of large files

Just write two mappers (multiple inputs): one for the first file and one for the second file that contains the two set of records to be joined.

for both file, emit couple of key-value where key=`JoinAttribute` and value=`"JoinAttribute: " + ValueToDisplayInJoinResult`. Then, in the reducer, we can perform the join.


# Data cleaning

to clean input data in order to have only relevant data for the analysis it's possible to use cleaning tools and libraryes such as `StemmerLucene`