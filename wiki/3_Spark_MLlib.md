# Data Mining Fundamentals

Knowledge discovery process pipeline:

1) **Preprocess Data**: real world data is dirty, garbage in garbage out

*  Data cleaning: reduce noise, remove outliers, solves inconsistencies
*  Data integration: renconciles data from different sources, integrates metadata, solve data value conflict, manages redundancy

2) **transform Data**
3) **data mining / machine learning** to found common patterns
4) **extract knowledge**, interpretation of results

Analysis Techniques:

1) **Descriptive**: extract interpretable model describing data, ex: customer segmentation
2) **Predictive**: exploits some known variables to predict unknown future values of other variables. ex: spam email detection


## Classification

**Objective**: prediction of a class label. Allow to define an interpretable model of a given phenomenon. 

**Approaches**: decision trees, bayesian classification, classification rules, neural network, k-nearest neighbours, support vector machine...


## Regression

**Objective**: prediction of a continuois value (similar to classification but in a continuous space), e.g. prediction of stock quotes


## Clustering 

**Objective**: detect group of similar data object. Identify exceptions and outliers.

**Approaches**: K-means, DBSCAN...


## Association rules

**Objective**: extraction of frequent correlations or pattern from a transactional database.

**Metrics**: given two itemsets A and B, associated with the tule A=>B (A imply B) we can define

* **Support**: the fraction os transaction containing both A and B: #{A,B}/|T| where |T| is the cardinality of the transactional database

* **Confidence**: the frequency of B in transactions containing A: sup(A,B)/sup(A), equal to the conditional probability of finding B having found A. Can be seen as a the streght of the "imply" rule

**Approaches**: based on exploratory techniques to find correlation between items that appears in the transactions. Given a set of transactions T, association rule mining is the extraction of the rules that satisfy the constraints:

* `support >= minsup threshold`
* `confidence >= minconf threshold`



# **Spark MLlib**

 A Spark component providing the ML and DM algorithms: pre-processing techniques, classification (supervised), clustering (unsupervised) and Itemset mining algorithms are implemented in this library.

 MLlib APIs are divided into two packages, the second version is `org.apache.spark.ml`: it provied high-level API built on top of DataFrames that allow to implement ML pipeline.

 Definition of **feature** in the ML/DM context:

```
A  measurable and individual property of an observed phenomenon.
```

It's crucial to select from the input datasets only the relevant features to use as input records for the ML/DM algorithms.


 ## Data Types

 MLlib is based on a set of basic local and distributed data types, DataFrames for ML contains objects based on this basic data types:


 #### Local vectors

Vectors are use to store the input features. 

defined in `org.apache.spark.ml.linalg.Vector`, are used to store vectors of double values. both **dense and sparse vectors** are supported. MLlib work only with vectors of double, so non double features must be mapped to double values using this class: one vector for each input record.

Two representation are handles by spark, e.g. for the vector (1.0, 0.0, 3.0):

1) **dense format**: [1.0, 0.0, 3.0]
2) **sparse format**: (3, [0,2], [1.0, 3.0]), so 3 is the number of elements, then indexes of non empty cells, then list of values of non empty cells. This representation is useful to store sparse vectors/matrix without wasting space.

```
Vector dv = Vector.dense(1.0, 0.0, 3.0); //istantiation of a dense vector
Vector sv = Vector.sparse(3, new[] {0,2}, new double[]{1.0,3.0});//sparse vector
```

#### Labeled points

They're used by supervised ML algorithms (classification and regression) to represents the records/data points. 

Defined in `org.apache.spark.ml.feature.LabeledPoint`. Defines a local vector associated with a label. The label is a Double value:

* For **classification problem**: each class label is associated to an integer from 0 to C-1, where C is equal to the number of distinct classes.
* For **regression problem**, the label is the real value to predict

```
//first parameter class label, second parameter feature vector

// Create a LabeledPoint for the first record/data point
LabeledPoint record1 = new LabeledPoint(1, Vectors.sparse(3, new int[] {0, 2}, new double[] {1.0, 3.0}) );

// Create a LabeledPoint for the second record/data point
LabeledPoint record2 = new LabeledPoint(0, Vectors.sparse(3, new int[] {0, 1, 2}, new double[] {2.0, 5.0, 3.0}) );

Double lab = record1.label(); //returns the label
Vector v = record1.features(); // returns the features vector
```

#### Sparse labeled data

Because frequently the training data are sparse, MLlib supports reading trainig examples stored in the LIBSVM format:

```
1 1:5.8 2:1.7 
0 1:4.1 3:2.5 4:1.2
```

It means that the first document is associated with class label 1 and it contains the words associated with index 1 and with index 2, and report the weight associated with each word

To read data in this format we can use:

```
//for java rdd, to return a DataFrame don't call toJavaRDD() method
JavaRDD<LabeledPoint> example = MLUtils.loadLibSVMFile(sc.sc(), "sample_lisbsvm_data.txt").toJavaRDD();
```



# MLlib, main concepts

Spark ML use DataFrames from spark SQL as ML datasets (they are alis of Dataset<Row>), which can hold variety of data types, e.g. can store columns containing text, feature vectors, labels, predictions. So all the input data must be represented by means of "tables" before applying the MLlib algorithms, also the document collections must be firstly transformed in a tabular format.

The DataFrames used by the MLlib algorithms are characterized by several columns associated with different characteristics of the input data:

* **label**: target of a classification/regression analysis
* **features**: a Vector containing the values of the features of the input record
* **text**: the original text of a document before being transformed in a tabular format

### Transformer

It's a ML procedure that can transform one DataFrame into another one. Similar to spark transformations, they're executed lazily. 

### Estimator

A ML algorithm than can be applied to a DataFrame to produce a Transformer (so, a model). It abstract the concept of a learning algorithm, or any algorithm that fits or trains on an input dataset and returns a model.

### Pipeline

It chains multiple transformers and estimators together to specify a machine learning workflow.

**Steps of a pipeline approach**:

1) the set of transformers and estimators is instantiated
2) a pipeline object is created and the sequence of transformers and estimators associated with the pipeline are specified
3) the pipeline is executed and a model is created
4) the model is applied on new data


# Classification Algorithms

supported by Spark MLlib:

* logistic regression
* decision trees
* SVM with linear kernel
* Naive bayes, implement always this one first because it gives good results.

All algorithms are based on two phases:

* **model generation** based on a set of training data. Spark only works on numerical  attributes, so categorical values must be mapped to integer numbers before applying the MLlib classification algorithm.
* **prediction of the class label** of new unlabeled data

It's always better to cache the content of the DataFrame used as training set, because the training is analyzed many times in the fit() method.

All the Spark classification algorithms are built on top of an input Dataset<Row> containing at least two columns:

* **label**, the attribute to be predicted by the classification model
* **features**, a vector of doubles containing the values of the predictive attributes of the input records.



## **Logistic Regression** and structured Data

1. Specify the class label
2. Specify the other attributes that are used to characterize the data
3. infer the model

Code sample:

```
...read the RDD...
/*
* Training phase
*/

//create a LabeledPoint RDD
JavaRDD<LabeledPoint> trainingRDD = trainingData.map(record ->{

    String[] fields = record.split(",");

    double classLabel = Double.parseDouble(fields[0]);
    double[] attributesValues = new double[3];
    attributesValues[0] = Double.parseDouble(fields[1]);
    attributesValues[1] = Double.parseDouble(fields[2]);
    attributesValues[2] = Double.parseDouble(fields[3])

    // Create a dense vector based on the content of attributesValues
    Vector attrValues = Vectors.dense(attributesValues);

    // Return a LabeledPoint based on the content of the current line
    return new LabeledPoint(classLabel, attrValues);
});

//preare training data
Dataset<Row> training = ss.createDataFrame(trainingRDD, LabeledPoint.class).cache();

//create a LogisticRegression object
LogisticRegression lr = new LogisticRegression();
lr.setMaxIter(10); //number of iterations
lr.setRegParam(0.01); //regularization parameter

//Define the pipeline, int contains only one stage
Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] {lr});

//train the model
PipelineModel model = pipeline.fit(training);

/*
* Prediction phase
*/

//map unlabeled data to LabeledPoint

JavaRDD<LabeledPoint> unlabeledRDD=unlabeledData.map(record ->{

String[] fields = record.split(",");

double[] attributesValues = new double[3];
attributesValues[0] = Double.parseDouble(fields[1]);
attributesValues[1] = Double.parseDouble(fields[2]);
attributesValues[2] = Double.parseDouble(fields[3]);
Vector attrValues= Vectors.dense(attributesValues);

double classLabel = -1; //class label is unknown

return new LabeledPoint(classLabel, attrValues);
})

//create the Dataframe
Dataset<Row> test = ss.createDataFrame(unlabeledRDD, LabeledPoint.class);

//make predictions
Dataset<Row> predictions = model.transform(test);

//select only relevant informations
Dataset<Row> predictionsDF = predictions.select("features", "prediction");

predictionsDF.javaRDD().saveAsTextFile(outputPath);

...

```



## **Decision Trees** and structured data

a decision tree is a tree used to obtain a label for each input data, the labels are the leaves. The tree is traversed from the root to the leaves, assigning the label to each input data.


Code sample:
```
...first part is identical to logistic regression LR...

DecisionTreeClassifier dc = new DecisionTreeClassifier();
dc.setImpurity("gini");
Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] {dc});
PipelineModel model = pipeline.fit(training);

/* prediction step */
...obtain the JavaRDD<LabeledPoints> in the same manner as LR...

Dataset<Row> predictions = model.transform(test);

//select relevant informations
Dataset<Row> predictionsDF = predictions.select("features", "prediction");
...
```


## Categorical class labels

Frequently the class label is a categorical value, so a String. But Spark MLlib works only with numerical values, so the categorical values must be mapped to integer (and then double) values. 

Two **Estimators** are available to peform this transformations: **StringIndexer** and **IndexToString**.

NOTE: If the *class labels are not available* for the input dataset, just set the categoricalLabel attribute to one of the possible values.


Code sample:

```
//implement a custom class for the LabeledPoint

public class MyLabeledPoint implements Serializable {
    
    private String categoricalLabel;
    private Vector features;
    
    public MyLabeledPoint(String categoricalLabel, Vector features) {
        this.categoricalLabel = categoricalLabel;
        this.features = features;
    }

    public String getCategoricalLabel() {return categoricalLabel;}

    public Vector getFeatures() {return features;}

    public void setFeatures(Vector features) {this.features = features;}
}

...

JavaRDD<String> trainingData = sc.textFile(inputFileTraining); //read the training file

//convert the input RDD to an RDD of MyLabeledPoints
JavaRDD<MyLabeledPoint> trainingRDD = trainingData.map(record -> {

    String[] fields = record.split(",");
    
    // Fields of 0 contains the id of the class
    String classLabel = fields[0];
    
    // The other three cells of fields contain the values of the three predictive attributes
    double[] attributesValues = new double[3];
    attributesValues[0] = Double.parseDouble(fields[1]);
    attributesValues[1] = Double.parseDouble(fields[2]);
    attributesValues[2] = Double.parseDouble(fields[3]);
    Vector attrValues = Vectors.dense(attributesValues);
    
    return new MyLabeledPoint(classLabel, attrValues);
});

//convert to DataFrame
Dataset<Row> training = ss.createDataFrame(trainingRDD, MyLabeledPoint.class).cache();


//prepare the StringIndexer, map values of "categoricalLabel" to integers stored in new column "label"
StringIndexerModel labelIndexer = new StringIndexer()
    .setInputCol("categoricalLabel")
    .setOutputCol("label")
    .fit(training);

//create new classifier
DecisionTreeClassifier dc = new DecisionTreeClassifier();
dc.setImpurity("gini");

//prepare the IndexToString,remap numerical prediction to String class labels
IndexToString labelConverter = new IndexToString() 
    .setInputCol("prediction")
    .setOutputCol("predictedLabel")
    .setLabels(labelIndexer.labels());

//create the pipeline with the correct order of stages
Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] {labelIndexer,dc,labelConverter});

//execute the pipeline to obtain the classified model
PipelineModel model = pipeline.fit(training);


...read the input of unlabeled data in the same manner of the training data...

//apply the model on the Dataset<Row> of unlabeled data and select relevant column
Dataset<Row> predictions = model.transform(unlabeled);
Dataset<Row> predictionsDF = predictions.select("features", "predictedLabel");

```

### Reading from LIBSVM file format

If the input data are stored in a LIBSVM file, that is a file that contains **sparse data** in a dense format, just use this chain of methods:

```
Dataset<Row> data = ss.read().format("libsvm").load("libsvm_data.txt");
```

The obtained Dataset<Row> has two columns:

* **label**: Double value associated with the label
* **features**: a sparse Vector associated with the predictive features.


#
## Textual data classification with Logistic Regression

Allow to create a classification model based on the logistic regression algorithm for **textual documents**: each line contains a class label and a list of words (the text of the document).

As with all ML algorithms, the words must be translated into double values. 

* remove using a list of stopwords useless words (e.g. conjuctions), use `Tokenizer` and `StopWordsRemover` class
* remove words that appear in almost all documents, they do not characterize data! e.g. Hello at the beginning of a mail doesn't help to recognize spam or ham.
* use TF-IDF (Term Frequency - Inverse Document Frequency) measure to assign a weight to the words based on their frequency in the collection. Use `HashingTF` to select a word, apply an hash that returns an integer, it also allow to reduce the complexity of the problem by specifying the maximum number of features allowed using the `setNumFeatures(int num)` method.

All three steps are to be implemented as part of the ML pipeline. They accept as input a an object composed of a Double and a String.


Code sample:
```
//write a class used to store the labeled document

public class LabeledDocument implements Serializable {
    private double label;
    private String text;
    
    public LabeledDocument(double label, String text) {
        this.text = text;
        this.label = label;
    }

    public String getText() {return this.text;}
    public void setText(String text) {this.text = text;}
    public double getLabel() {return this.label;}
    public void setLabel(double label) {this.label = label;}
}

...

/*training step*/
JavaRDD<String> trainingData = sc.textFile(inputFileTraining);

JavaRDD<LabeledDocument> trainingRDD = trainingData.map(record -> {
    String[] fields = record.split(",");

    double classLabel = Double.parseDouble(fields[0]); //class label
    String text = fields[1]; //content of the document
    return new LabeledDocument(classLabel, text);
}).cache();

Dataset<Row> training = ss.createDataFrame(trainingRDD, LabeledDocument.class).cache();

/*pipeline definition*/
Tokenizer tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words");
StopWordsRemover remover = new StopWordsRemover().setInputCol("words").setOutputCol("filteredWords");
HashingTF hashingTF = new HashingTF().setNumFeatures(1000)
    .setInputCol("filteredWords")
    .setOutputCol("rawFeatures");

IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features"); //apply IDF transformation
LogisticRegression lr = new LogisticRegression().setMaxIter(10).setRegParam(0.01);

Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] {tokenizer, remover, hashingTF, idf, lr});
PipelineModel model = pipeline.fit(training);

/*prediction step*/
...read data in the same manner as unlabeled input, just set classLable attribute to -1...

Dataset<Row> unlabeled = ss.createDataFrame(unlabeledRDD, LabeledDocument.class);
Dataset<Row> predictions = model.transform(unlabeled);
Dataset<Row> predictionsDF = predictions.select("text", "prediction");
```


#
## Tuning the parameters of a Classification algorithm (LR)

Cross validation can be used to test the accuracy of a classification algorithm and the quality of the model. In spark the CrossValidator class allow to use Cross Validation to retrieve the best parameters possible. 

Code sample
```
...after having created the pipeline object as always, i.e. for linear regression...

//construct a grid of parameter values to search over
ParamMap[] paramGrid = new ParamGridBuilder()
    .addGrid(lr.maxIter(), new int[]{10, 100, 1000})
    .addGrid(lr.regParam(), new double[]{0.1, 0.01})
    .build();

//create the cross validator object
CrossValidator cv = new CrossValidator()
    .setEstimator(pipeline)
    .setEstimatorParamMaps(paramGrid)
    .setEvaluator(new BinaryClassificationEvaluator())
    .setNumFolds(3);

//run cross-validation, the result is the LR model based on the best set of parameters
CrossValidatorModel model = cv.fit(training);

...then this model can be used to evaluate training data...
```



#
# Clustering Algorithms

Spark MLlib provides a limited set of clustering algorithms compares to other tools such as RapidMiner:

* **K-means**, it's a good solution for datasets where the data are separated in large groups, that can be shaped as centroids in a cartesian plane. K is the number of clusters.

* **Gaussian mixture**


### K-means in Spark MLlib

1) create the input JavaRDD of String reading the input file
2) map the JavaRDD<String> to a JavaRDD<Row> using the InputRecord class, which create a Vector from the input row data and then use returns a GenericRow object created from the Vector: `return new GenerciRow(vect)`
3) define a DataFrame based on the input data, using the JavaRDD of Rows and another object to specify the schema of the rows. (another way to generate a DataFrame)
4) instantiate a `KMeans` object and set the value of K with `setK(int k)`.
5) create the pipeline
6) invoke `pipeline.fit(data)` to obtain the PipelineModel
7) now call the PipeLineModel transform method on the data to obtain the DataFrame of the clustered data


Code sample
```
//define the InputRecord class
public class InputRecord implements Serializable {
    private Vector features;

    public Vector getFeatures() {return features;}
    public void setFeatures(Vector features) {this.features = features;}
    public InputRecord(Vector features) {this.features = features;}
}

...read the RDD...

//map and convert to Dataset
JavaRDD<InputRecord> inputRDD = inputData.map(record -> {
    String[] fields = record.split(",");
    
    // The three cells of fields contain the (numerical) values of the three input attributes.
    double[] attributesValues = new double[3];
    attributesValues[0] = Double.parseDouble(fields[0]);
    attributesValues[1] = Double.parseDouble(fields[1]);
    attributesValues[2] = Double.parseDouble(fields[2]);
    
    Vector attrValues = Vectors.dense(attributesValues);
    return new InputRecord(attrValues);
});

Dataset<Row> data = ss.createDataFrame(inputRDD, InputRecord.class).cache();

//create clustering pipeline
KMeans km = new KMeans();
km.setK(2);
Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] {km});

PipelineModel model = pipeline.fit(data);
Dataset<Row> clusteredData = model.transform(data);

```


# Itemset and Association rule Mining

Spark provides:

* an itemset mining algorithm based on the FP-growth algorithm, that extract all the sets of items of any length with a minimum frequency
* a rule mining algorithm that extract the association rules with a minimum frequency and a minimum confidence

In this cases, the input dataset is a set of transactions. Each transation is defined a set of items, e.g:

```
A B C D
A B
B C
A D E
```

in this example there are 4 transactions, example of itemsets: A, B, C, D, ABCD, AB, BC... if the min support threshold is 2, then only the ABCD itemset is not returned because it appear only one time


## FP-Growth algorithm

Is characterized by only one parameter: the minimum support threshold (minsup). All the item with minsup below the threshold are pruned.

it allow to extract the frequent itemset. then combining the frequent itemset we can extract the association rule.

Exploits a data structure called FP-tree, the mining is performed by a recursive visit of the tree, applying divide-et-impera approach


## Association Rule Mining algorithm

Given a set of frequent itemsets, the frequent association rule can be mined. They're used to extract information from the data, e.g. customers habits.

the algorithm is characterized by two parameters:

* min support threshold
* min confidence threshold

Code Sample:

```
...
minSupport = Double.parseDouble(args[3]);
minConfidence = Double.parseDouble(args[4]);
...

JavaRDD<String> inputData = sc.textFile(inputFile);
JavaRDD<Transaction> inputRDD = inputData.map(line -> {
    String[] items = line.split(" ");
    return new Transaction(Arrays.asList(items));
});

Dataset<Row> transactionsData = ss.createDataFrame(inputRDD, Transaction.class).cache();

//create FPgrowth, assign parameters, and fit input data
FPGrowth fp = new FPGrowth();
fp.setMinSupport(minSupport).setMinConfidence(minConfidence);
FPGrowthModel itemsetsAndRulesModel = fp.fit(transactionsData);

// Retrieve the set of frequent itemsets
Dataset<Row> frequentItemsets = itemsetsAndRulesModel.freqItemsets();
// Retrieve the set of association rules
Dataset<Row> frequentRules = itemsetsAndRulesModel.associationRules();
```



# Regression algorithms

similar to classification, but we don't want to predict a categorical attribute, but a numeric attribute, so a **continuous value**. E.g. predict the price of a stock from historical values.

Spark provides:

* Linear regression, similar to logistic regression but with continuous data

All the available algorithms works only on numerical data, so categorical values must be mapped to integer numbers. So the dataset must be transformed in a DataFrame of LabeledPoints.

Code sample
```
...
//read input data, E.g.: 1.0,5.8,0.51.7
JavaRDD<String> trainingData = sc.textFile(inputFileTraining);

// Map each input record/data point of the input file to a LabeledPoint
JavaRDD<LabeledPoint> trainingRDD=trainingData.map(record ->{
    String[] fields = record.split(",");
    
    double targetLabel = Double.parseDouble(fields[0]);

    double[] attributesValues = new double[3];
    attributesValues[0] = Double.parseDouble(fields[1]);
    attributesValues[1] = Double.parseDouble(fields[2]);
    attributesValues[2] = Double.parseDouble(fields[3]);
    Vector attrValues= Vectors.dense(attributesValues);
    
    return new LabeledPoint(targetLabel , attrValues);
});

Dataset<Row> training = ss.createDataFrame(trainingRDD, LabeledPoint.class).cache(); 

//create Linear Regrssion object
LinearRegression lr = new LinearRegression();
lr.setMaxIter(10);
lr.setRegParam(0.01);

Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] {lr});
PipelineModel model = pipeline.fit(training);

..read data to be tested in the same manner as input data...

//make predictions
Dataset<Row> predictions = model.transform(test);
Dataset<Row> predictionsDF = predictions.select("features", "prediction");

```











