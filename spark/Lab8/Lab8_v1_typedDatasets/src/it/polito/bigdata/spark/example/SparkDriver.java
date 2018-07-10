package it.polito.bigdata.spark.example;

import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.Column;


public class SparkDriver {

	public static void main(String[] args) {

		String inputPath;
		String inputPath2;
		Double threshold;
		String outputFolder;

		inputPath = args[0]; //register.csv
		inputPath2 = args[1]; //stations.csv
		threshold = new Double(args[2]);
		outputFolder = args[3];

		// Create a Spark Session object and set the name of the application
		SparkSession ss = SparkSession.builder().appName("Spark Lab #8 - Template").getOrCreate();

		//read the the file register.csv into a DataFrame
		DataFrameReader dfr = ss.read().format("csv").option("header", true).option("inferSchema", true);
		Dataset<Row> df = dfr.load(inputPath);
		
		
		//create a Dataset from the DataFrame
		Encoder<RegisterRow> registerRowEncoder = Encoders.bean(RegisterRow.class);
		
		Dataset<RegisterRow> rrDs = df.as(registerRowEncoder);
		
		//filter the input deleting unused rows
		Dataset<RegisterRow> filteredDs = rrDs.filter(row -> {
			
			if(row.getFree_slots()!=0 && row.getUsed_slots()!=0)
				return true;
			else return false;
		});
		
		//map the dataset to a new dataset similar to a word count problem
		Dataset<StationTimeslot> stDS = filteredDs.map(
			row -> {
				
				StationTimeslot st = new StationTimeslot();
				
				st.setStation(row.getStation());
				
				//create timeslot
				String day = DateTool.DayOfTheWeek(row.getTimestamp());
				String[] spl = row.getTimestamp().split("\\s+");
				String time = spl[2];
				String[] splTime = time.split(":");
				String hour = splTime[0];
				
				String timeslot = day + " - " + hour;
				st.setTimeslot(timeslot);
				
				//set counters
				st.setEntryCount(1);
				
				if(row.getFree_slots() == 0)
					st.setEntryFreeSlotZeroCount(1);
				else st.setEntryFreeSlotZeroCount(0);
				
				return st;
			},
			Encoders.bean(StationTimeslot.class)
		);
				
		//group by station-timeslot and aggregate the counters for each pair
		RelationalGroupedDataset grouped = stDS.groupBy("station", "timeslot");
		Dataset<Row> groupedStatistics = grouped
				.agg( sum("entryCount"), sum("entryFreeSlotZeroCount"))
				.toDF("station", "timeslot", "numReadings", "numZeroReadings");
		
				
		//compute the critical statistics
		Dataset<StationTimeslotCritical> criticalStatistics = groupedStatistics.map(
				row -> {
					
					StationTimeslotCritical stc = new StationTimeslotCritical();
					
					stc.setStation(row.getString(0));
					stc.setTimeslot(row.getString(1));
					Float criticality = new Float(row.getInt(3))/row.getInt(2);
					stc.setCriticality(criticality);
					
					return stc;
				},
				Encoders.bean(StationTimeslotCritical.class)
		);	
		
		//select only the rows that are above the threshold
		Dataset<StationTimeslotCritical> filteredCriticalStatisticts = criticalStatistics.filter( stat -> {
			if(stat.getCriticality() > threshold)
				return true;
			else return false;
		});
		
		//join with stations csv to obtain for each station its coordinates.
		Dataset<Row> coordinatesDS = dfr.load(inputPath2)
				.select("id", "longitude", "latitude") //name not needed
				.toDF("station", "longitude", "latitude");
		
		Encoder<StationLocation> stationLocationEncoder = Encoders.bean(StationLocation.class);
		Dataset<StationLocation> stationLocationDS = coordinatesDS.as(stationLocationEncoder);
		
		Dataset<Row> joinedDS = filteredCriticalStatisticts
				.join(stationLocationDS, filteredCriticalStatisticts.col("station").equalTo(stationLocationDS.col("station")))
				.selectExpr("station", "timeslot", "criticality", "latitude", "longitude");
				
		//order the results
		Dataset<Row> sortedRes = joinedDS.sort(new Column("criticality").desc(), new Column("station"), new Column("timeslot"));
		
		//store result in output folder
		sortedRes.write().format("csv").option("header", true).save(outputFolder);
		
		
		// Close the Spark session
		ss.stop();
	}
}
