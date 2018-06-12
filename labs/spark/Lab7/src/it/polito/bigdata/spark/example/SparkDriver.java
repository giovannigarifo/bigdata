package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath;
		String inputPath2;
		Double threshold;
		String outputNameFileKML;

		inputPath = args[0];
		inputPath2 = args[1];
		threshold = new Double(args[2]);
		outputNameFileKML = args[3];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Lab #7");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Read the content of the input file register.csv
		JavaRDD<String> inputRDD = sc.textFile(inputPath);

		//filter only the relevant entries from the input file
		JavaRDD<String> filteredInputRDD = inputRDD.filter( record -> {
			
			boolean isRelevant = true;
			
			if(record.startsWith("station"))
				isRelevant = false;
			
			String[] splittedRecord = record.split("\\t+");
			if(splittedRecord[3].equals("0") && splittedRecord[4].equals("0"))
				isRelevant = false;
			
			return isRelevant;
		});
		
		//map the record to a Tuple2 element of this kind: Key=stationId_timeslot, Value= usedSlots_freeSlots
		JavaPairRDD<String,String> stationTimeslotRDD = filteredInputRDD.mapToPair( record -> {
						
			String[] splittedRecord = record.split("\\t+");

			//generate stationId
			String stationId = splittedRecord[0];
			
			//generate timeslot
			String timestamp = splittedRecord[1];
			String[] splittedTimestamp = timestamp.split("\\s");
			
			String day = DateTool.DayOfTheWeek(splittedTimestamp[0]);
			
			String[] splittedTime = splittedTimestamp[1].split(":"); 
			String hour = splittedTime[0];
			
			String timeSlot = day + " - " + hour;
			
			//generate key, i.e. "1_Wednesday - 15"
			String key = stationId + "_" + timeSlot;
			
			//generate value, i.e. "3_17"
			String value = splittedRecord[2] + "_" + splittedRecord[3];
			
			
			return new Tuple2<String,String>(key,value);
		});
		
		//group by key, to obtain an iteratable of all the the reading associated to a specific timeslot for each station
		JavaPairRDD<String, Iterable<String>> stationTimeslotGroupedRDD = stationTimeslotRDD.groupByKey();
		
		//compute the total for each key
		JavaPairRDD<String, Float> stationTimeslotCriticalRDD = stationTimeslotGroupedRDD.mapValues( slotsList -> {
			
			//We can assume that the number of elements is less than 720 (a reading every 2 minutes, for every day)
			int totZeroFreeSlot = 0;
			int tot = 0;
			
			for( String slot : slotsList) {
				
				String[] slotInfo = slot.split("_");
				
				if(slotInfo[0].equals("0"))
					totZeroFreeSlot++;
				
				tot++;		
			}
			
			//compute criticality for the station Si at the specific timeSlot		
			return new Float(totZeroFreeSlot/tot);
		});
		
		//filter to maintain only the values above treshold
		JavaPairRDD<String,Float> filteredCriticalRDD = stationTimeslotCriticalRDD.filter( record -> {
			return record._2() > threshold;
		});
		
		//select the pair(s) with the maximum critical value;
		JavaPairRDD<String, Float> maxCriticalRDD = filteredCriticalRDD.reduceByKey((val1, val2) -> {
		
			if(val1>=val2)
				return val1;
			else return val2;
		});
		
		/*
		 * Select the most critical timeslot for each station
		 */
		
		//obtain a new pairRDD <StationID, timeslot_criticalValue>
		JavaPairRDD<String, String> mostCriticalPerStation = maxCriticalRDD.mapToPair( pair -> {
			
			String[] pairKey = pair._1().split("_");
			
			return new Tuple2<String, String>(pairKey[0], pairKey[1] + "_" + pair._2());
		});
		
		
		//select for each station the most critical timeslot
		JavaPairRDD<String, String> mostcriticalTimeslotPerStation = mostCriticalPerStation.reduceByKey((val1,val2) -> {
			
			//val 1
			String[] splittedVal1 = val1.split("_");
			String timeSlot1 = splittedVal1[0];
			String day1 = timeSlot1.split(" - ")[0];
			Integer hour1 = new Integer(timeSlot1.split(" - ")[1]);
			
			//val 2
			String[] splittedVal2 = val2.split("_");
			String timeSlot2 = splittedVal2[0];
			String day2 = timeSlot2.split(" - ")[0];
			Integer hour2 = new Integer(timeSlot2.split(" - ")[1]);
			
			//compare them by day and hour, take only one
			if(hour1>hour2)
				return val1;
			else if(hour2>hour1)
				return val2;
			else if(hour1==hour2) {
				if((int) day1.compareTo(day2) > 0)
					return val1;
				else if((int)day2.compareTo(day1) > 0)
					return val2;
			}
			
			//if they are the same, take just one
			return val1; 
		});
		
		
		//return to the previous RDD and store the value
		JavaPairRDD<String, Float> defineteltyMostCritical = mostCriticalPerStation.mapToPair( pair -> {
			
			String[] splittedValue = pair._2().split("_");
						
			return new Tuple2<String, Float>(pair._1() + "_" + splittedValue[0], new Float(splittedValue[1]));
		});
		
		
		
		
		

		// Store in resultKML one String, representing a KML marker, for each station 
		// with a critical timeslot 
		// JavaRDD<String> resultKML = ;
		JavaRDD<String> resultKML = null;
		
		// There is at most one string for each station. We can use collect and
		// store the returned list in the main memory of the driver.
		List<String> localKML = resultKML.collect();
		
		// Store the result in one single file stored in the distributed file
		// system
		// Add header and footer, and the content of localKML in the middle
		Configuration confHadoop = new Configuration();

		try {
			URI uri = URI.create(outputNameFileKML);

			FileSystem file = FileSystem.get(uri, confHadoop);
			FSDataOutputStream outputFile = file.create(new Path(uri));

			BufferedWriter bOutFile = new BufferedWriter(new OutputStreamWriter(outputFile, "UTF-8"));

			// Header
			bOutFile.write("<kml xmlns=\"http://www.opengis.net/kml/2.2\"><Document>");
			bOutFile.newLine();

			// Markers
			for (String lineKML : localKML) {
				bOutFile.write(lineKML);
				bOutFile.newLine();
			}

			// Footer
			bOutFile.write("</Document></kml>");
			bOutFile.newLine();

			bOutFile.close();
			outputFile.close();

		} catch (IOException e1) {
			e1.printStackTrace();
		}

		// Close the Spark context
		sc.close();
	}
}
