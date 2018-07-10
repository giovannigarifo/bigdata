package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;

public class SparkDriver {

	@SuppressWarnings("resource")
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

		// Read the content of the input file
		JavaRDD<String> inputRDD = sc.textFile(inputPath);

		// Remove the header and the lines with #free slots=0 && #used slots=0
		JavaRDD<String> filteredRDD = inputRDD.filter(line -> {
			// Remove header
			if (line.startsWith("s") == true) {
				return false;
			} else {
				String[] fields = line.split("\\t");
				int usedSlots = Integer.parseInt(fields[2]);
				int freeSlots = Integer.parseInt(fields[3]);

				// Select the lines with freeSlots!=0 || usedSlots!=0
				if (freeSlots != 0 || usedSlots != 0) {
					return true;
				} else
					return false;
			}
		});

		// Map each line to a pair
		// key = StationId_DayOfTheWeek_Hour
		// value = (1,1) if the station is full, (1,0) if the station is not
		// full
		JavaPairRDD<String, CountTotReadingsTotFull> stationWeekDayHour = filteredRDD.mapToPair(line -> {
			// station timestamp used free
			// 1 2008-05-15 12:01:00 0 18

			String[] fields = line.split("\\t");
			int freeSlots = Integer.parseInt(fields[3]);

			String[] timestamp = fields[1].split(" ");
			String dayOfTheWeek = DateTool.DayOfTheWeek(timestamp[0]);
			String hour = timestamp[1].replaceAll(":.*", "");

			CountTotReadingsTotFull countRF;

			if (freeSlots == 0) {
				// The station is full
				countRF = new CountTotReadingsTotFull(1, 1);
			} else {
				// The station is not full
				countRF = new CountTotReadingsTotFull(1, 0);
			}

			return new Tuple2<String, CountTotReadingsTotFull>(fields[0] + "_" + dayOfTheWeek + "_" + hour, countRF);
		});

		// Count the total number of readings and "full" readings for each key
		JavaPairRDD<String, CountTotReadingsTotFull> stationWeekDayHourCounts = stationWeekDayHour.reduceByKey(
				(element1, element2) -> new CountTotReadingsTotFull(element1.numReadings + element2.numReadings,
						element1.numFullReadings + element2.numFullReadings));

		// Compute criticality for each key
		JavaPairRDD<String, Double> stationWeekDayHourCriticality = stationWeekDayHourCounts
				.mapValues(value -> new Double((double) value.numFullReadings / (double) value.numReadings));

		// Select only the pairs with criticality > threshold
		JavaPairRDD<String, Double> selectedPairs = stationWeekDayHourCriticality.filter(value -> {
			double criticality = value._2().doubleValue();

			if (criticality >= threshold) {
				return true;
			} else {
				return false;
			}
		});

		// The next part of the code selects for each station the timeslot
		// (dayOfTheWeek_Hour) with
		// the maximum cardinality. If there is more than one timeslot with the
		// same criticality
		// for the same station, only one of them is selected (see the problem
		// specification).

		// Create a new PairRDD with
		// key = DayOfTheWeek - Hour
		// value = Criticality
		JavaPairRDD<String, DayOfWeekHourCrit> stationTimeslotCrit = selectedPairs
				.mapToPair(StationDayWeekHourCount -> {
					// (2_Sat_02,402)
					String[] fields = StationDayWeekHourCount._1().split("_");
					String stationId = fields[0];
					String dayWeek = fields[1];
					String hour = fields[2];

					Double criticality = StationDayWeekHourCount._2();

					return new Tuple2<String, DayOfWeekHourCrit>(stationId,
							new DayOfWeekHourCrit(dayWeek, Integer.parseInt(hour), criticality));
				});

		// Select the timeslot (dayOfTheWeek_Hour) with the maximum criticality
		// for each station
		JavaPairRDD<String, DayOfWeekHourCrit> resultRDD = stationTimeslotCrit
				.reduceByKey((DayOfWeekHourCrit value1, DayOfWeekHourCrit value2) -> {
					if (value1.criticality > value2.criticality
							|| (value1.criticality == value2.criticality && value1.hour < value2.hour)
							|| (value1.criticality == value2.criticality && value1.hour == value2.hour
									&& value1.dayOfTheWeek.compareTo(value2.dayOfTheWeek) < 0)) {
						return new DayOfWeekHourCrit(value1.dayOfTheWeek, value1.hour, value1.criticality);
					} else {
						return new DayOfWeekHourCrit(value2.dayOfTheWeek, value2.hour, value2.criticality);
					}
				});

		// Read the location of the stations
		JavaPairRDD<String, String> stationLocation = sc.textFile(inputPath2).mapToPair(line -> {
			// id latitude longitude name
			// 1 41.397978 2.180019 Gran Via Corts Catalanes
			String[] fields = line.split("\\t");

			return new Tuple2<String, String>(fields[0], fields[1] + "," + fields[2]);
		});

		// Join the locations with the "critical" stations
		JavaPairRDD<String, Tuple2<DayOfWeekHourCrit, String>> resultLocations = resultRDD.join(stationLocation);

		// Create a string containing the description of a marker, in the KML
		// format, for each
		// sensor and the associated information
		JavaRDD<String> resultKML = resultLocations
				.map((Tuple2<String, Tuple2<DayOfWeekHourCrit, String>> StationMax) -> {

					String stationId = StationMax._1();

					DayOfWeekHourCrit dWHC = StationMax._2()._1();
					String coordinates = StationMax._2()._2();

					String result = "<Placemark><name>" + stationId + "</name>" + "<ExtendedData>"
							+ "<Data name=\"DayWeek\"><value>" + dWHC.dayOfTheWeek + "</value></Data>"
							+ "<Data name=\"Hour\"><value>" + dWHC.hour + "</value></Data>"
							+ "<Data name=\"Criticality\"><value>" + dWHC.criticality + "</value></Data>"
							+ "</ExtendedData>" + "<Point>" + "<coordinates>" + coordinates + "</coordinates>"
							+ "</Point>" + "</Placemark>";

					return result;
				});

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
