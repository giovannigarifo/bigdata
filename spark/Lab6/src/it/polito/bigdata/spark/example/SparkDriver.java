package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;
import java.io.Serializable;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.SparkConf;
	
public class SparkDriver {
	
	public static void main(String[] args) {

		String inputPath;
		String outputPath;
		
		inputPath=args[0];
		outputPath=args[1];

	
		// Create a configuration object and set the name of the application
		SparkConf conf=new SparkConf().setAppName("Spark Lab #6");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		
		// Read the content of the input file
		JavaRDD<String> inputRDD = sc.textFile(inputPath);
		
		JavaRDD<String> filteredInputRDD = inputRDD.filter( record -> {
			if(record.startsWith("Id,")) 
				return false; //throw away the record
			else return true; //take the record
		});

		// map the inputRDD to a PairRDD of <uid, pid>
		JavaPairRDD<String, String> inputPairRDD = filteredInputRDD.mapToPair( record -> {
		
			String[] splittedRecord = record.split(",");
			String uid = splittedRecord[2];
			String pid = splittedRecord[1];
						
			return new Tuple2<String, String>(uid, pid);			
		});
		
		
		// group by key to obtain <uid, list of pid>, groupbykey not optimail solution!
		// better to use combineByKey
		JavaPairRDD<String, Iterable<String>> userProductsPairRDD = inputPairRDD.groupByKey();
		
		
		//get only the pids that are the interesting parts
		JavaRDD<Iterable<String>> pidsRDD = userProductsPairRDD.values();
		
		//map the pids to pairs <pid1_pid2, 1>
		JavaPairRDD<String, Integer> pidsCouplesRDD = pidsRDD.flatMapToPair( pids -> {
			
			ArrayList<Tuple2<String,Integer>> tuples = new ArrayList<>();
			
			for (String p1 : pids) {
                for (String p2 : pids) {
                    if (p1.compareTo(p2) > 0) //test if not equals and if wrong order (e.g. pid2_pid1)
                        tuples.add(new Tuple2<String, Integer>(p1 + "_" + p2, 1));
                }
            }
			
			return tuples.iterator();
		});
		
		//reduce by key to obtain <pid1_pid2, #occurrences>
		JavaPairRDD<String, Integer> pidsCouplesOccurrencesRDD = pidsCouplesRDD.reduceByKey( (count1, count2) -> {
			return count1+count2;
		});
		
		// remove all the couples that appear only one time
		JavaPairRDD<String, Integer> pidsCouplesOccurrencesFilteredRDD = pidsCouplesOccurrencesRDD.filter( couple -> couple._2() > 1 );
		
		//swap key and value to allow sorting on the occurences
		JavaPairRDD<Integer, String> swappedResultRDD = pidsCouplesOccurrencesFilteredRDD.mapToPair( pids_occurrencies -> {
		
			return new Tuple2<Integer,String>(pids_occurrencies._2, pids_occurrencies._1);
		});
		
		//sort in descending order
		JavaPairRDD<Integer,String> sortedResultRDD = swappedResultRDD.sortByKey(false);
		
		// Store the result in the output folder
		sortedResultRDD.saveAsTextFile(outputPath);
		
		/**
		 * Bonus track: print to stdout the top10 most frequent pairs and their frequencies
		 */
		
		List<Tuple2<String,Integer>> top10 = pidsCouplesOccurrencesRDD.top(10, 
				(Comparator<Tuple2<String,Integer>> & Serializable) (couple1, couple2 ) -> {
	
					return couple1._2().compareTo(couple2._2());
		});
		
		//save as file instead of stdout
		//JavaRDD<Tuple2<String,Integer>> top10RDD = sc.parallelize(top10);
		//top10RDD.saveAsTextFile("top10output");
		
		//print to stdout
		System.out.println("\n\n\n|-|-|- PRINTING OUTPUT -|-|-|\n");
		for(int i=0; i<top10.size(); i++)
			System.out.println(top10.get(i)._1 + " " + top10.get(i)._2());
		

		// Close the Spark context
		sc.close();
	}
}
