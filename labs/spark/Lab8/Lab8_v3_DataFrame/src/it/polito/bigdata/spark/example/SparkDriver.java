package it.polito.bigdata.spark.example;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
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

		// Read the content of the input file register.csv and store it into a
		// DataFrame
		// The input file has an header
		// Schema of the input data:
		// |-- station: integer (nullable = true)
		// |-- timestamp: timestamp (nullable = true)
		// |-- used_slots: integer (nullable = true)
		// |-- free_slots: integer (nullable = true)
		Dataset<Row> inputDF = ss.read().format("csv").option("delimiter", "\\t").option("header", true)
				.option("inferSchema", true).load(inputPath);

		// Remove the lines with #free slots=0 && #used slots=0
		Dataset<Row> filteredDF = inputDF.filter("free_slots<>0 OR used_slots<>0");

		// Define a User Defined Function called full(Integer free_slots)
		// that returns 1 if the value of
		// free_slots is equal to 0.
		// 1 if free_slots is greater than 0
		ss.udf().register("full", (Integer free_slots) -> {
			if (free_slots == 0)
				return 1;
			else
				return 0;
		}, DataTypes.IntegerType);

		// Define a User Defined Function that returns the DayOfTheWeek
		// given a Timestamp value
		ss.udf().register("day_of_week", (Timestamp date) -> DateTool.DayOfTheWeek(date), DataTypes.StringType);

		// Define a DataFrame with the following schema:
		// |-- station: integer (nullable = true)
		// |-- dayofweek: string (nullable = true)
		// |-- hour: integer (nullable = true)
		// |-- fullstatus: integer (nullable = true) - 1 = full, 0 = non-full

		Dataset<Row> stationWeekDayHourDF = filteredDF.selectExpr("station", "day_of_week(timestamp) as dayofweek",
				"hour(timestamp) as hour", "full(free_slots) as fullstatus");

		// Define one group for each combination "station, dayofweek, hour"
		RelationalGroupedDataset rgdstationWeekDayHourDF = stationWeekDayHourDF.groupBy("station", "dayofweek", "hour");

		// Compute the criticality for each group (station, dayofweek, hour),
		// i.e., for each pair (station, timeslot)
		// The criticality is equal to the average of fullStatus
		Dataset<Row> stationWeekDayHourCriticalityDF = rgdstationWeekDayHourDF.agg(avg("fullStatus"))
				.withColumnRenamed("avg(fullStatus)", "criticality");

		// Select only the lines with criticality > threshold
		Dataset<Row> selectedPairsDF = stationWeekDayHourCriticalityDF.filter("criticality>" + threshold);

		// Read the content of the input file stations.csv and store it into a
		// DataFrame
		// The input file has an header
		// Schema of the input data:
		// |-- id: integer (nullable = true)
		// |-- longitude: double (nullable = true)
		// |-- latitude: double (nullable = true)
		// |-- name: string (nullable = true)
		Dataset<Row> stationsDF = ss.read().format("csv").option("delimiter", "\\t").option("header", true)
				.option("inferSchema", true).load(inputPath2);

		// Join the selected critical "situations" with the stations table to
		// retrieve the coordinates of the stations
		Dataset<Row> selectedPairsCoordinatesDF = selectedPairsDF.join(stationsDF,
				selectedPairsDF.col("station").equalTo(stationsDF.col("id")));

		Dataset<Row> selectedPairsIdCoordinatesCriticalityDF = selectedPairsCoordinatesDF
				.selectExpr("station", "dayofweek", "hour", "longitude", "latitude", "criticality")
				.sort(new Column("criticality").desc(), new Column("station"), new Column("dayofweek"),
						new Column("hour"));

		selectedPairsIdCoordinatesCriticalityDF.show();
		
		
		// Save the result in the output folder
		selectedPairsIdCoordinatesCriticalityDF.write().format("csv").option("header", true)
				.save(outputFolder);


		// Close the Spark session
		ss.stop();
	}
}
