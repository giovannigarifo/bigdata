package it.polito.bigdata.spark.example;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;

import java.sql.Timestamp;

import org.apache.spark.sql.types.DataTypes;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath;
		String inputPath2;
		Double threshold;
		String outputFolder;

		inputPath = args[0];
		inputPath2 = args[1];
		threshold = new Double(args[2]);
		outputFolder = args[3];

		// Create a Spark Session object and set the name of the application
		SparkSession ss = SparkSession.builder().appName("Spark Lab #8 - Template").getOrCreate();

		//read the the file register.csv into a DataFrame
		DataFrameReader dfr = ss.read().format("csv").option("header", true).option("inferSchema", true).option("delimiter", "\\t");
		Dataset<StationReading> stations = dfr.load(inputPath).as(Encoders.bean(StationReading.class));
		
		//assign a table to the dataset
		stations.createOrReplaceTempView("stationReadings");
		
		//define a UDF that computes from a timestamp the day of the week
		ss.udf().register("day_of_week", 
				(Timestamp timestamp) -> DateTool.DayOfTheWeek(timestamp), 
				DataTypes.StringType);
		
		//define a UDF that returns 0 or 1 for the condition on free_slots
		ss.udf().register("station_full", 
				(Integer free_slots) -> free_slots == 0 ? 1 : 0,
				DataTypes.IntegerType);

		
		//select only the lines with free_slots!=0 and used_slots!=0,
		//group the reading by station and weekday-hour
		//select only the groups above criticality
		//return the station, timeslot, criticality
		Dataset<StationDayHourCriticality> stationsCriticality = ss.sql(
				"SELECT station, day_of_week(timestamp) as day, hour(timestamp) as hour, avg(station_full(free_slots)) as criticality"
				+ "FROM stationReadings "
				+ "WHERE free_slots<>0 AND used_slots<>0 "
				+ "GROUP BY station, day_of_week(timestamp), hour(timestamp) "
				+ "HAVING avg(station_full(free_slots)) >" + threshold + ";"
		
		).as(Encoders.bean(StationDayHourCriticality.class));;
		
		//assign table to stationsCriticality
		stationsCriticality.createOrReplaceTempView("criticalStations");
		
		//read the location data
		Dataset<StationLocation> stationsLocations = dfr.load(inputPath).as(Encoders.bean(StationLocation.class));
		
		//assign table to statiosnLocations
		stationsLocations.createOrReplaceTempView("stationsLocations");
		
		//join the two tables
		Dataset<FinalRecord> result = ss.sql(
				"SELECT station, day, hour, longitude, latitude, criticality"
				+ "FROM criticalStations, stationsLocations "
				+ "WHERE criticalStations.station == stationsLocations.id "
				+ "ORDER BY criticality DESC, station, dayofweek, hour"
		).as(Encoders.bean(FinalRecord.class));
	

		// Close the Spark session
		ss.stop();
	}
}
