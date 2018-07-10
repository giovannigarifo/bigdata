#  Apache Spark

## Introduction

Highly optimized framework for data analysis via iterative jobs (the opposite of Hadoop): the mappers write on main memory and the reducers read from main memory, so that there isn't network bandwidth overhead. Main memory is shared between all the clusters just as the HDFS in Hadoop.

Spark motivations:

* The necessity to use MapReduce for complex iterative jobs
* RAM cost decreasing
* it was necessary a solution to keep more data in main memory: The basis of Spark 
* Analysis of not only batch data (static analysis), but also streaming  and interactive data (real time analysis)

### Resilient Distributed Data Sets (RDDs)

Basic reprensentation of data in a Spark application. They're manipulated through a set of parallel trasformations and actions (Not only map and reduce) and hide the problem of synchronization in a parallel execution.


## Spark Architecture

The Spark Core is expanded from several components that implements the necessary abstractions to analyze different type of data:

* SQL structured Data
* Streaming real-time data
* MLlib
* GraphX (not covered)

Spark applications can be developed and tested on local machine by means of Standalone Spark Scheduler, making more easy to debug applications with respect to the Hadoop ones.

Input data sets are splitted and each chunk is stored in the main memory of one node of the cluster: the level of parallelism depends on the number of splits. Every node can be seed as a "Worker" that runs an "Executor" process, a local JVM that do it's job a split of the input data.


RDDs can be created:

* by parallelizing existing collections. In this case the number of partitions can be defined by the user.
* reading from a distributed file system, e.g. HDFS. In this case the number of partitions, so, the number of executors aka workers is the number of HDFS blocks, same as MapReduce for the number of mappers.



# Spark applications basics

Supports many different languages, e.g. Python, Scala (used to implement the Spark framework itself), Java.

### Structure of a Spark application

* **Driver**

Contains the workflow of the application, instantiating a `JavaSparkContext` object that define the configuration, and creating the RDDs and invoking parallel operations on RDDs.

```
// Create a configuration object and set the name of the application
SparkConf conf = new SparkConf().setAppName(“Application
name"); 

// Create a Spark Context object
JavaSparkContext sc = new JavaSparkContext(conf);
```

The driver defines two type fo data: local Java variables and RDDs

* **Executor**

a java VM that runs on a Worker node, each executor works a different chunk of the RDDs.

If we execute the application locally, each Worker is a thread.


# Spark RDDs transformations and actions

A Spark RDD is an **immutable distributed collection of objects**.


## How to create and save RDDs

Note that the data in spark are lazily read only when they're needed, e.g. when actions are applied. This apply to all the available methods below.


#### 1) Creating an RDD from a text file. 

RDD can be created reading from a text input file.

```
JavaRDD<String> lines = sc.textFile(inputFile); //each element of the RDD is a line of the input file
```

There is also an overload of the textFile method which accept as second paramter the number of partitions into which split the input file, uesful when reading from the local file system to tune the application parallelization.


#### 2) creating an RDD from a local collection

This is the only case in which we can define explicitly the number of splits.

```
JavaRDD<String> distributedList = sc.parallelize(inputList); // one element of the RDD for each element of the local list.
```

Also in this case is possible to call an overload of the parallelize method which accept the number of partitions as second parameter.


#### 3) Save RDDs to mass storage

simply call `linesRDD.saveAsTextFile(outputPath);`. Each line is stored in a line of the output file.


#### 4) Save RDDs to local Java variables

can be achieved using the collect() method. **Make sure that the RDD is not ginormous!**

```
List<String> contentOfLines = linesRDD.collect(); //each element of the RDD of Strings is an element of the lits of strings
```

The type of the List objects must be the same of the one's of the JavaRDD.



#
## Transformations

Are operations on RDDs that return a new RDD, this because RDDs are *immutable*.

Transformations are not applyed immediately, Spark only records how to create the RDD and then it will apply the transformation when needed (lazy operation), e.g. a method is applyed to the RDD. If data are not really needed, they're not readed. This is one of main architectural advantages of Spark.

The lazy approach is necessary for spark to be able to optimize the execution (just like in SQL) by analyzing the DAG of the application (called *lineage graph*).

Available transformations, they're very similar to java stream methods:

* **Filter**

```
JavaRDD<String> inputRDD = sc.textFile("log.txt");
JavaRDD<String> errorsRDD = inputRDD.filter(line -> if(line.contains("error")) return true);
```

The lambda defines on the fly the implementation of the `call()` method of the `Function<String,Boolean>` interface required by spark to implement the filter transformation.


* **Map**: map an object into another. The new RDD contains exactly one element for each element of the input RDD, it's a mapping 1-to-1.

```
JavaRDD<String> inputRDD = sc.textFile("usernames.txt");
// Compute the lengths of the input surnames
JavaRDD<Integer> lenghtsRDD = inputRDD.map(surname -> new Integer(surname.length());
```


* **FlatMap**: same as map but for each element can return a set of elements, the merge of all the returned sets will be the final result (a single list and not a list of lists!), so it's a mappping 1-to-many.

```
JavaRDD<String> inputRDD = sc.textFile("document.txt");
// Compute the list of words occurring in document.txt
JavaRDD<String> listOfWordsRDD = inputRDD.flatMap(x ->Arrays.asList(x.split(" ")).iterator());
```

The new RDD contains the “concatenation” of the lists obtained by applying the lambda function over all the elements of inputRDD. The new RDD is an RDD of Strings and not of List<String>.

The lambda implements the `public Iterable<R> call(T element)` method of the `FlatMapFunction<T,R>` interface.


* **Distinct**: doesn't need to be implemented, just call it. It's a very expensive operation, use it only when it's really needed because it need to send data over the network between nodes. For the shuffle operation to be performed for each distinct value a new partition is created.

```
// Select the distinct names occurring in inputRDD
JavaRDD<String> distinctNamesRDD = inputRDD.distinct();
```

* **Sample**, returns an RDD containing a random sample of the elements of the input one. Implemented by the method `JavaRDD<T> sample(boolean withReplacement, double fraction)`, fraction specify the expected size of the sample as a fraction of the input RDD size.

```
JavaRDD<String> randomSentencesRDD = inputRDD.sample(false,0.2);
```

## Set Transformatios

All these transformations have two input RDDs, one is the RDD on which the method is invoked, the other one is passed as parameter to the method, and only one output RDD

* **Union**, implemented by `JavaRDD<T> union(JavaRDD<T> secondInputRDD)`. doesn't remove duplicates, if it's really necessary use distinct on the output RDD.

* **Intersection**, expensive. implementd by `JavaRDD<T> intersection(JavaRDD<T> secondInputRDD)` method of the JavaRDD<T> class.

* **Substract**, expensive. `JavaRDD<T> subtract(JavaRDD<T> secondInputRDD)`

* **Cartesian product**, very expensive for the same reason above but also for the complexity of the resulting set. Implemented by `JavaPairRDD<T, R> cartesian(JavaRDD<R> secondInputRDD)` of the JavaRDD<T> class. The two input RDDs can have different data type, the returned RDD is a JavaPairRDD containing all the combinations composed of one element of the first RDD and one of the second RDD.

Intersection and Union are the only transformations that allows to merge two different RDDs.

```
// create two RDDs of integers
List<Integer> inputList1 = Arrays.asList(1, 2, 3);
JavaRDD<Integer> inputRDD1 = sc.parallelize(inputList1);

List<Integer> inputList2 = Arrays.asList(3, 4, 5);
JavaRDD<Integer> inputRDD2 = sc.parallelize(inputList2);

// Create three new RDDs by using union, intersection, and subtract
JavaRDD<Integer> outputUnionRDD = inputRDD1.union(inputRDD2);
JavaRDD<Integer> outputIntersectionRDD = inputRDD1.intersection(inputRDD2);
JavaRDD<Integer> outputSubtractRDD = inputRDD1.subtract(inputRDD2);

// Compute the cartesian product
JavaPairRDD<Integer, Integer> outputCartesianRDD = inputRDD1.cartesian(inputRDD2);
```


#
## Actions

Operations that return a result to the driver, in a local java variable. Pay attention to the size when returning data to the driver!

Available actions for a Spark RDD:

* **Collect**, pay attention to the size of the RDD because the result of collect is saved in a local variable. If the size is to big, sae the RDD to a text file and then read it again.

```
JavaRDD<Integer> inputRDD = sc.parallelize(Arrays.asList(1, 2, 3, 3));
// Retrieve the elements of the inputRDD and store them in a local Java list
List<Integer> retrievedValues = inputRDD.collect();
```

* **Count**, counts the lines of an RDD

```
long numLinesDoc1 = inputRDD1.count();
```


* **CountByValue**, returns a java.util.Map collection that is stored in a local variable. Counts the number of times each element of the RDD occurs, similar to a wordcount.

```
java.util.Map<String, java.lang.Long> namesOccurrences = namesRDD.countByValue();
```

* **Take**, get the first n elements of the RDD, similar to a topk. Returns always a list, even if n=1.

```
List<Integer> firstTwoElem = inputRDD.take(2);
```

* **First**, a take with n=1. Returns always a single value.

* **Top**, returns a local java list containing the top n elements, objects stored in the RDD must implement the Comparable interface.

```
JavaRDD<Integer> inputRDD = sc.parallelize(Arrays.asList(1, 5, 3, 3, 2));
// Retrieve the top-2 elements of the inputRDD and store them in a local Java list
List<Integer> retrievedValues = inputRDD.top(2);

...
//from lab6 bonus track, implements compare method of comparable interface via serializable lambda

List<Tuple2<String,Integer>> top10 = pidsCouplesOccurrencesRDD.top(
    10, 
	(Comparator<Tuple2<String,Integer>> & Serializable) (couple1, couple2 ) -> {
	
		return couple1._2().compareTo(couple2._2());
	}
);


```

* **TakeOrdered**, retrieve top n smallest elements of the considered RDD. Also in this case the objects stored in the RDD must implement the Comparable interface. Implemented by `List<T> takeOrdered (int n, java.util.Comparator<T>comp)` method of the JavaRDD<T> class.


* **TakeSample**, similar to the transformation Sample, it's used to get a subset of the RDD and store it in a local variables

```
JavaRDD<Integer> inputRDD = sc.parallelize(Arrays.asList(1, 5, 3, 3, 2));
List<Integer> randomValues= inputRDD.takeSample(true, 2);
```

* **Reduce**, used to combine the elements of the RDD, the result is an object of the same type of the input ones. the function used to reduce must be associative and commutative, so that it can be executed in parallel without problems. The `public T call(T element1, T element2)` method of the `Function2<T,T,T>` interface must be implemented in the lambda.

```
// Create an RDD of integers. Load the values 1, 2, 3, 3 in this RDD
JavaRDD<Integer> inputRDDReduce = sc.parallelize(Arrays.asList(1, 2, 3, 3));

// Compute the sum of the values;
Integer sum = inputRDDReduce.reduce((element1, element2) -> element1+element2);

//compute the max
Integer max = inputRDDReduce.reduce((element1, element2) -> {
    if (element1>element2)
        return element1;
    else
    return element2;
});
```

* **Fold**, identical to reduce, the only difference is that you must specify an initial value equal to 0. useless.


* **Aggregate**, returns a single Java object by combining the objects of the RDD and an initial "zero" value by using two user defined functions, that must be associative and commutative. The returned objects and the ones of the input RDD can be istances of different classes, this is the main difference with respect to reduce. The aggregate action is based on the `U aggregate(U zeroValue, Function2<U,T,U> seqOp, Function2<U,U,U> combOp)` method of the JavaRDD<T> class.

The first lambda must implement the `public U call(U element1, T element2)` method of the `Function2<U, T, U>` interface.

The second lambda must implement the `public U call(U element1, U element2)` method of the `Function2<U, U, U>` interface.

```
// Define a class to store two integers: sum and numElements
class SumCount implements Serializable {
    public int sum; public int numElements;
    
    public SumCount(int sum, int numElements) {
        this.sum = sum; this.numElements = numElements;
    }

    public double avg() { return sum/ (double) numElements;}
}

...

JavaRDD<Integer> inputRDDAggr = sc.parallelize(Arrays.asList(1, 2, 3, 3));

// Instantiate the zero value
SumCount zeroValue = new SumCount(0, 0);

// Compute sum and number over the elements of inputRDDAggr
SumCount result = inputRDDAggr.aggregate(zeroValue,

    //lambda used to combine the elements of the input RDD with the zero value and with the intermediate objects
    (a, e) -> {  
        a.sum = a.sum + e;
        a.numElements = a.numElements + 1;
        return a;
    },

    //used to combine the partial results emitted by the executors
    (a1, a2) -> {
        a1.sum = a1. sum + a2.sum;
        a1.numElements = a1.numElements + a2.numElements;   
        return a1;
    }
);

```


N.B.: to take advantage of parallelism we must use RDDs and associated transformation/actions. operation upon local variables are simple java code executed locally.


#
## Pair RDDs

RDDs of key-value pairs. Allow key grouping. can be created from an existing RDD with a `mapToPair()` transformation or from a java in-memory collection by using the `parallelizePairs()` method of the Spark Context.

java doesn't have a specific class for tuple, so we'll use `scala.Tuple2<K,V>` class. Scala and Java classes are interchangable because once compiled they have the same bytecode. key and value can be retrieved with methods `_1()` and `_2()`.

* **mapToPair**, the lambda must implement the `public Tuple2<K,V> call(T element)` method of the `PairFunction<T,K,V>` interface. Always use mapToPair even for **transformation from a PairRDD to a new PairRDD**.

```
JavaRDD<String> namesRDD = sc.textFile("first_names.txt");

JavaPairRDD<String, Integer> nameOneRDD = namesRDD.mapToPair(name ->
    new Tuple2<String, Integer>(name, 1)
);
```




* **flatMapToPair**, same as mapToPair but map each object to a set of Iterable objects.

* **parallelizePairs**

```
ArrayList<Tuple2<String, Integer>> nameAge = new ArrayList<Tuple2<String, Integer>>();
nameAge.add(new Tuple2<String, Integer>("Paolo", 40));

JavaPairRDD<String, Integer> nameAgeRDD = sc.parallelizePairs(nameAge);
```


## Transformations specific for Pair RDDs

other then the already mentioned transformations, this ones can be applied to a Pair RDD:

* **ReduceByKey**, similar to reduce but this is a transformation and NOT an action! so it returns a JavaRDD and is executed lazily. Note that data are sended over the network for the shuffle phase.

```
// Select for each name (key) the lowest age (value)
JavaPairRDD<String, Integer> youngestPairRDD = nameAgeRDD.reduceByKey(
    (age1, age2) -> {
        if (age1<age2)
            return age1;
        else
            return age2;
    }
);
```

The returned JavaPair contains one pair for each distinct input key. 


* **CombineByKey**, returns a new RDD where there is only one pair for each distinct key, the value associated with this key is computed by the lambda.

```
JavaPairRDD<String, AvgCount> avgAgePerNamePairRDD = nameAgeRDD.combineByKey(
        
        //given an integer, returns ad AvgCount object
        inputElement -> new AvgCount(inputElement, 1),
        
        //given an Integer and an AvgCount object, it combines them and returns an AvgCount object
        (intermediateElement, inputElement) -> {
            AvgCount combine=new AvgCount(inputElement, 1);
            combine.total=combine.total+intermediateElement.total;
            combine.numValues = combine.numValues+intermediateElement.numValues;
            return combine;
        },
        
        //given two AvgCount objects, it combines them and returns an AvgCount object
        (intermediateElement1, intermediateElement2) -> {
            AvgCount combine = new AvgCount(intermediateElement1.total,
            intermediateElement1.numValues);
            combine.total=combine.total+intermediateElement2.total;
            combine.numValues=combine.numValues+intermediateElement2.numValues;
            return combine;
        }
);
```

* **GroupByKey**, very expensive, try to avoid and use the other two

```
// Create one group for each name with the associated ages
JavaPairRDD<String, Iterable<Integer>> agesPerNamePairRDD = nameAgeRDD.groupByKey();
```

* **MapValues**, used to map only the values of the Pair RDD. One pair is created in the returned PairRDD for each input pair. The key is equal, the value is obtained by applying the lambda.

```
// Increment age of all users
JavaPairRDD<String, Integer> nameAgePlusOneRDD = nameAgeRDD.mapValues(age -> new Integer(age+1));
```

* **FlatMapValues**, just llike for Flat Map, for each key, the system will return a set of values (Iterable<T> object).

* **Keys**, returns the list of keys as a JavaRDD<K>, duplicates are not removed.

* **Values**, return the list of values as a JavaRDD<V>, duplicates are not removed.

* **SortByKey**, returns a new PairRDD obtained by sorting in ascending order the pairs of the input PairRDD by key.

```
JavaPairRDD<String, Integer> sortedNameAgeRDD = nameAgeRDD.sortByKey();
```


## Transformation on pairs of PairRDDs


Spark also supports some transformations that work on two PairRDDs

* **SubstractByKey**, select only pairs of the first RDD that doesn't appears in the second PairRDD. The key type must be the same, the value types can be different.

```
JavaPairRDD<String, Integer> profilesPairRDD = sc.parallelizePairs(profiles); // <Surname, Age>
JavaPairRDD<String, String> bannedPairRDD = sc.parallelizePairs(banned); // <Surname, BanMotivation>

// Select the profiles of the “good” users
JavaPairRDD<String, Integer> selectedUsersPairRDD = profilesPairRDD.subtractByKey(bannedPairRDD);
```

* **Join**, perform a join based on the value of the key of the pairs, each key of the PairRDD is combined with all the pairs of the other PairRDD with the same key, the new PairRDD has the same key type of input PairRDDs and has a tuple as value: the pair of values of the two joined input pairs. 

```
// Join questions with answers
JavaPairRDD<Integer, Tuple2<String, String>> joinPairRDD = questionsPairRDD.join(answersPairRDD);
```

* **CoGroup**, associate each key K of the input pairRDDs with the list of values associated with K in the input PairRDD and the list of values associated with the K key in the other PairRDD.

```
// Cogroup movies and directors per user
JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<String>>> cogroupPairRDD = moviesPairRDD.cogroup(directorsPairRDD);
```



## Actions specific for Pair RDDs

For all this action attention must be payed to the cardinality of the generated Java local variables.

* **CountByKey**, returns a map that is stored in local variable java.lang.Map (pay attention to the cardinality of the keys)

```
JavaPairRDD< String, Integer> movieRatingRDD = sc.parallelizePairs(movieRating);
java.util.Map<String, java.lang.Object> movieNumRatings = movieRatingRDD.countByKey();
```

* **CollectAsMap**, returns the Pair RDD as a java Map, be careful that will raise an error if in the Pair RDD there are tuple with the same key! use Collect if you know that there are elements with same key, it will return a list of Tuple2.

```
// Retrieve the content of usersRDD and store it in a local Java Map
java.util.Map<String, String> retrievedPairs = usersRDD.collectAsMap();
```

* **Lookup**, returns as local java List the values of the pairs associated with the K key specified as parameter

```
// Select the ratings associated with “Forrest Gump”
java.util.List<Integer> movieRatings = movieRatingRDD.lookup("Forrest Gump");
```



## DoubleRDDs and basic statistical measures

Spark provides specific **actions** for a specific numerical type of RDD: **JavaDoubleRDD**, that is not simply an RDD of Double objects.

Every JavaRDD can be trasformed in a JavaDoubleRDD using two specific transformations:

* **mapToDouble**

```
// Compute the lengths of the surnames
JavaDoubleRDD lenghtsDoubleRDD = surnamesRDD.mapToDouble(surname -> (double)surname.length());
```

* **flatMapToDouble**

```
// Create a JavaDoubleRDD with the lengths of words occurring in sentencesRDD
JavaDoubleRDD wordLenghtsDoubleRDD = sentencesRDD.flatMapToDouble(sentence -> {
    String[] words=sentence.split(" ");
    
    // Compute the length of each word
    ArrayList<Double> lengths=new ArrayList<Double>();
    for (String word: words) {
        lengths.add(new Double(word.length()));
    }
    return lengths.iterator();
});
```

* **parallelizeDoubles**, can transform a local java List<Double> into a JavaDoubleRDD.

### Actions specific for DoubleRDDs

* **sum**, **mean**, **stdev**, **variance**, **max**, **mix**. 

this actions can be used to obtain statistics over the whole set of elements of the DoubleRDD, if the desired outputs are statistic e.g. for each key, it's necessary to use a standard RDD with the reduce transformation.



# Cache, accumulators and broadcast variables


## Persistence and Cache

Every time an action is performed over an RDD or one of its descendants, Spark computes the content of the RDD. If the same RDD in used multiple times (e.g. more than one action is executed over the RDD) this can be an overhead.

We can force Spark to cache each partition locally on each executor in order to avoid reading again from the HDFS for every action using storage levels. 

Constants to define storage level:

* MEMORY_ONLY
* MEMORY_AND_DISK
* MEMORY_ONLY_SER
* MEMORY_AND_DISK_SER
* DISK_ONLY (local disk not hdfs)
* MEMORY_ONLY_2, EMORY_AND_DISK_2...for fault tolerance replicates each partition two times (2x data overhead sended over the network)

To mark an RDD as persistent (cached), use this transformation:

* `JavaRDD<T> persist(StorageLevel.CONSTANT)`
* `cache()`, equivalent to persist(StorageLevel.MEMORY_ONLY)
* `unpersist()`, to remove an RDD from che cache

Both **returns a new persistent RDD** bechause RDDs are immutable! Note this are transformations, so they're lazily executed.

Usually caching is usefull if we read an RDD more then 10 times, instead it's useless because it only introduce another overhead.



## Accumulators

Just as MapReduce Counters can be used to produce statistics, e.g. count the number of elements. They're shared variables that can be only incremented (to avoid synchronization problems).

They're defined and initialized in the Driver. In the worker nodes they'll be incremented, then the final value of the accumulator is returned to the driver. 

Note that accumulator are increased in the call method of the functions associated with transformations, because transformation are lazily executed, if you not execute an action over the RDD the transformation wll not take place and so the accumulator will not be incremented!

```
// in the driver
final Accumulator<Integer> invalidEmails = sc.accumulator(0);
...

// then in the transformation function
invalidEmails.add(1);
...
```



## Broadcast variables

Is a read-only (final) large shared variable between all the worker nodes, e.g. can be used to share a lookup-table or a dictionary. The size is limited to the amout of main memory available in each worker.

```
// Read the content of the dictionary from the first file and map each line to a pair (word, integer value)
JavaPairRDD<String, Integer> dictionaryRDD = sc.textFile("dictionary.txt").mapToPair(line -> {
    String[] fields = line.split(" ");
    String word=fields[0];
    Integer intWord = Integer.parseInt(fields[1]);
    return new Tuple2<String, Integer>(word, intWord) ;
});

//create a local dictionary
HashMap<String, Integer> dictionary = new HashMap<String, Integer>();

//populate the local dictionary
for (Tuple2<String, Integer> pair: dictionaryRDD.collect()) {
    dictionary.put(pair._1(), pair._2());
}

//create broadcast dictionary
final Broadcast<HashMap<String, Integer>> dictionaryBroadcast = sc.broadcast(dictionary);
```



# Spark SQL

A Spark component for structured data processing, doesn't support nested queries and other advanced SQL constructs. It's based on a SQL optimizer to optimize the queries, it provides an abstraction called **DataFrame** which is used to abstract the dataset on which must be performed the queries, in fact, a DataFrame is nothing more than a `Dataset<Row>` object.

* **Dataset**: a distributed collection of structured data, it combines the benefits of RDDs and the benefits of Spark SQL optimizing engine.

* **DataFrame**: a particular kind of Dataset organized into named column, it's *conceptually equivalent to a table* in a relational database.


First thing to do is to start a SparkSession, that offers all the functionalities of Spark SQL:

```
SparkSession ss = SparkSession.builder().appName("App.Name").getOrCreate();

//code...

ss.stop();
```

Then a DataFrame can be used to store a distributed version of a SQL table, so a collection of data organized into columns with a schema. Also the content of the DataFrame is lazily evaluated, the same of RDD transformations.

## 1) DataFrame

### DataFrame possible sources

* Read from **JSON**

```
DataFrameReader dfr=ss.read().format("json"); //.option("multiline",true) can be used to specify that is a standard multiline json

Dataset<Row> df = dfr.load("persons.json");
```

* Read from **CSV**

```
DataFrameReader dfr=ss.read().format("csv")
    .option("header", true) //used to specify that the first line of the file contains the **name** of the attributes/columns
    .option("inferSchema", true); //used to specify that the system must infer the **data types** of each column, without this option they're all considered strings
Dataset<Row> df = dfr.load("persons.csv");
```


* read from **existing RDDs**

write a class that implements the Serializable interface, this class must express the schema of the table. 
Then create a List of those object and use the parallelize() method over the context to retrieve a JavaRDD of those objects. Then create the DataFrame:

```
DataFrame personsDF = sqlCtx.createDataFrame(personsRDD, Person.class);
```


### From DataFrame to RDD

The `javaRDD()` method of the DataFrame class returns a `JavaRDD<Row>`. Each **Row** object is like a vector containing the schema and value of each row of the Dataframe.

Important methods of the Row class are:

* `int fieldIndex(String columnName)`, it returns the index of a given field/column name

* `java.lang.Object getAs(String columnName)` it retrieve the content of a field/column given its name

* `String getString(int position)`

* `double getDouble(int position)`


```
// Define an JavaRDD based on the content of the DataFrame
JavaRDD<Row> rddPersons = df.javaRDD();

// Use the map transformation to extract the name field/column
JavaRDD<String> rddNames = rddRows.map(inRow -> 
    (String)inRow.getAs("name"));

// Store the result
rddNames.saveAsTextFile(outputPath);
```



 ## 2) Datasets

Datasets are more general than DataFrames, in fact they're collections of objects. The schema of the represented data is consistent with the attributes of the class of the contained objects. DataFrame is an “alias” of Dataset<Row>. **Datasets are more efficient than RDDs**, thanks to a specialized encoder used for the serialization. They also offers type-safeness thanks to the compiler type-checking, due to the fact that the objects stored in a Spark Dataset must be **JavaBean-compliant**:

* they must implement Serializable interface
* all their attributes must have public setter/getters
* all their attributes must be private

Always use the getters and the setters to enforce type checking.



### Creating a Dataset

* **From local collection with custom Encoder**

```
ArrayList<Person> persons = new ArrayList<Person>();
//...populate the list

// Person class has been implemented as JavaBean-compliant
// Define the encoder that is used to serialize Person objects
Encoder<Person> personEncoder = Encoders.bean(Person.class);

// Define the Dataset based on the content of the local list of persons
Dataset<Person> personDS = ss.createDataset(persons, personEncoder);
```


 * **From local collection with default Encoder**
  
  some predefined encoder are available for the basic types and classes in the class **Encoders**: `Encoder<Integer> Encoders.INT()` and `Encoder<String> Encoders.STRING()`

  ```
// Encoders for most common types are provided in class Encoders
Dataset<Integer> primitiveDS = ss.createDataset(Arrays.asList(40, 30, 32), Encoders.INT());
  ```


* **From DataFrames**

It's possible by using the `Dataset<T> as(Encoder encoder)` method of a DataFrame.


```
// Read the content of the input file and store it into a DataFrame
DataFrameReader dfr=ss.read().format("csv").option("header",true).option("inferSchema", true).option("delimiter", "\\t");
Dataset<Row> df = dfr.load("persons.csv");

// Define the encoder that is used to serialize Person objects
Encoder<Person> personEncoder = Encoders.bean(Person.class);

// Define a Dataset of Person objects from the df DataFrame
Dataset<Person> ds = df.as(personEncoder);

```


* **From CSV or JSON files**

Simply define a DataFrame based on the input CSV/JSON file, and then convert it to Dataset using the method above. The **CSV delimiter** can be specified with this option: `.option("delimiter", "\\t")`


* **From Scala RDD**

It's possible to create a Dataset from a Scala RDD using the method `Dataset<T> createDataset(RDD<T> inRDD, Encoder<T> encoder)`



## Operations on Datasets and on DataFrames

Pay attention that some of this methods print to stdout, some returns a DataFrame (equivalent to Dataset<Row>) and some a Dataset.


* **show(int numrows)**, print to stdout the first numrows of the dataframe, used for debugging purposes.


* **printSchema()**, print to stdout the schema of the dataframe


* **long count()**, returns the number of tuples of the dataframe. count over a dataframe is faster compared to count over an RDD.


* **Dataset distinct()**, used to remove duplicate tuples. It sends data over the network, pay attention when using it.


* **DataFrame select(String col1, ... String coln)**, it's a projection which mantain duplicates. Returns a new DataFrame with only the specified columns. can generate error at runtime if there are mistakes in the names of the columns.


* **DataFrame selectExpr(String exp1, .., String expN)**, returns a new Dataset that contains a set of columns computed by combining the original columns.

```
Dataset<Row> df = ds.selectExpr("name", "age", "gender", "age+1 as newAge");
```


* **Dataset filter(String conditionExpr)** or its alias **Dataset where(String expression)**, like a select in SQL: the condition can use boolean algebra operator such as AND, OR, NOT... it select also the tuple which have NULL as value corresponding to the condition, example:

```
DataFrame greaterThan17 = dfProfiles.filter("age>17"); //select also if age==NULL, we can use "age>17 AND age IS NOT NULL"
```

* **Dataset filter(FilterFunction<T> func)** filters with a lambda function, only the element for echich the lambda returns true are present in the returning Dataset.

```
Dataset<Person> dsSelected = ds.filter(p -> {
    if (p.getAge()>=20 && p.getAge()<=31)
        return true;
    else
        return false;
});
```


* **Dataset\<U> map(scala.Function1\<T,U> func, Encoder\<U> encoder)**, each element of the resulting dataset is obtained by applying the function passed as parameter. The encoder parameter is used to encode the returning object of the function.

```
// Create a new Dataset containing name, surname and age+1 for each person
Dataset<PersonNewAge> ds2 = ds.map(
    p -> {
        PersonNewAge newPersonNA =
        new PersonNewAge();
        newPersonNA.setName(p.getName());
        newPersonNA.setAge(p.getAge());
        newPersonNA.setGender(p.getGender());
        newPersonNA.setNewAge(p.getAge()+1);
        return newPersonNA; 
    }, 
    Encoders.bean(PersonNewAge.class));
```


* **Dataframe join(Dataset<T> right, Column joinExprs)**, used to perform a **natural join** two datasets based on the given join expression.

```
Dataset<Row> dfPersonLikes = dsPersons.join(dsUidSports, dsPersons.col("uid").equalTo(dsUidSports.col("uid")));
```


* **agg(aggregate functions)** is used to apply an already Spark defined aggregate function such as avg(col), min(col), max(col) over the values of a specific column.

```
// Compute the average of age and the number of rows
Dataset<Row> avgAgeCount = ds.agg(avg("age"),count("*"));
```

* **RelationalGroupedDataset groupBy(String col1, .., String coln)** can be used to split the input data in groups and compute aggregate function over each group using the above method.

```
// Group data by name
RelationalGroupedDataset rgd = ds.groupBy("name");

// Compute the average of age for each group
Dataset<Row> nameAverageAge = rgd.avg("age");
```


* **Dataset<T> sort(String col1, .., String coln)** returns a new dataset with the content sorted by col1,...,coln in **ascending order**. Append the `desc()` method to sort in **descending order**.

```
// Create a new Dataset with data sorted by desc. age, asc. name
Dataset<Person> sortedAgeName = ds.sort(new Column("age").desc(), new Column("name"));
```



## Save Datasets and DataFrames


* **JavaRDD<T> javaRDD()**, return a corresponding RDD of T elements. Then use `saveAsTextFile()` to store the result.

```
ds.javaRDD().saveAsTextFile(outputPath);
```


* **DataFrameWriter<Row> write()** combined with `format(String filetype)` and `void save(String outputFolder)` method

```
ds.write().format("csv").option("header", true).save(outputPath);
```



## Datasets, DataFrames and the SQL language


Spark SQL also allows to query the content of a DataFrame by using the SQL language. In order to do this, a **table name must be assigned to each DataFrame**:

```
dfPeople.createOrReplaceTempView("people");
dfLiked.createOrReplaceTempView("liked");

DataFrame greaterThen17 = ss.sql("SELECT * FROM people WHERE age>17"); 
DataFrame dfPersonLikes = ss.sql("SELECT * from people, liked where people.uid=liked.uid");
DataFrame nameAvgAgeCnt = ss.sql("SELECT name, avg(age), count(name) FROM people GROUP BY name");
```

 it is better to also specify the list of columns rather then using *, because we can force the position of the column and this can be useful for the Row class methods.


## User Defined Functions (UDF)

It's possible to define custom functions to be used inside some transformation, ie.e the `selectExpr(..)` or `sort(..)`, but also inside the **SQL queries**.

* **udf().register(String name, UDF function, DataType datatype)** invoked by the SparkSession object can be used to register an UDF that can be later on invoked.

```
ss.udf().register("length", (String name) -> name.length(), DataTypes.IntegerType); //define the UDF

//then you can use it
Dataset<Row> result1 = inputDF.selectExpr("length(name) as size");
Dataset<Row> result2 = ss.sql("SELECT length(name) FROM profiles");
```


## User Defined Aggregate Functions (UDAF)

It's also possible to define personalized aggregate functions, they're used to aggregate the vlues of a set of tuples. They are based on the implementation of the `org.apache.spark.sql.expressions.UserDefinedAggregateFunction` abstract class.



 ## Dataset vs DataFrames vs SQL 

 1) With the SQL-like approach, error related to a wrong SQL query can happen. 
 2) With Dataset we cannot have runtime error, only compile errors.



