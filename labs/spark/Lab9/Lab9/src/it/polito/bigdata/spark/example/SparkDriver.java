package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.ml.feature.LabeledPoint;

public class SparkDriver {

	@SuppressWarnings("resource")
	public static void main(String[] args) {

		String inputPath;

		inputPath=args[0];
	
		
		// Create a Spark Session object and set the name of the application
		// We use some Spark SQL transformation in this program
		SparkSession ss = SparkSession.builder().appName("Spark Lab9").getOrCreate();

		// Create a Java Spark Context from the Spark Session
		// When a Spark Session has already been defined this method 
		// is used to create the Java Spark Context
		JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());

		
		
    	//EX 1: READ AND FILTER THE DATASET AND STORE IT INTO A DATAFRAME
		
		// To avoid parsing the comma escaped within quotes, you can use the following regex:
		// line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
		// instead of the simpler
		// line.split(",");
		// this will ignore the commas followed by an odd number of quotes.

		JavaRDD<String> input = sc.textFile(inputPath);
		
		//filter useless entries
		JavaRDD<String> filteredInput = input.filter( row -> {
			
			String[] splittedRow = row.split(",");
			
			if( row.startsWith("Id") || new Integer(splittedRow[5]) == 0 )
				return false;
			else return true;
		});
		
		//map to labeled point
		JavaRDD<LabeledPoint> lpRDD = filteredInput.map( row -> {
			
			String[] fields = row.split(",");
			
			//label
			Double helpfulnessDen = new Double(fields[5]);
			Double helpfulnessNum = new Double(fields[4]);
			Double helpfulnessIndex = helpfulnessNum/helpfulnessDen;
			double label = helpfulnessIndex >= 0.9? 1.0 : 0.0;
			
			//features
			double features[] = new double[1]; 
			features[0] = fields[9].length();
			/*features[1] = new Double(fields[7]);
			features[2] = new Double(fields[6]);*/
			
			Vector attrValues = Vectors.dense(features);
			
			
			return new LabeledPoint(label, attrValues);
		});
		
	
		Dataset<Row> schemaReviews = ss.createDataFrame(lpRDD, LabeledPoint.class).cache();
				
		// Display 5 example rows.
    	schemaReviews.show(5);
		
        // Split the data into training and test sets (30% held out for testing)
        Dataset<Row>[] splits = schemaReviews.randomSplit(new double[]{0.7, 0.3});
        Dataset<Row> trainingData = splits[0];
        Dataset<Row> testData = splits[1];

        
    	//EX 2: CREATE THE PIPELINE THAT IS USED TO BUILD THE CLASSIFICATION MODEL
        
        
        /* V1: Logistic regression*/
        //create the logistic regression object
        LogisticRegression lr = new LogisticRegression();
        lr.setMaxIter(10);
        lr.setRegParam(0.01);
        
        //define the pipeline
        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] {lr}); 

        // Train model. Use the training set 
        PipelineModel model = pipeline.fit(trainingData);
		
        
        /* V2: Decision Tree*/
        /*
         * DecisionTreeClassifier dc = new DecisionTreeClassifier();
        dc.setImpurity("gini");
        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] {dc});
        PipelineModel model = pipeline.fit(trainingData);
         */

		/*==== EVALUATION ====*/
		
		// Make predictions for the test set.
		Dataset<Row> predictions = model.transform(testData);

		// Select example rows to display.
		predictions.show(5);

		// Retrieve the quality metrics. 
        MulticlassMetrics metrics = new MulticlassMetrics(predictions.select("prediction", "label"));

        // Confusion matrix
        Matrix confusion = metrics.confusionMatrix();
        System.out.println("Confusion matrix: \n" + confusion);

        double accuracy = metrics.accuracy();
		System.out.println("Accuracy = " + accuracy);
            
        // Close the Spark context
		sc.close();
	}
}
