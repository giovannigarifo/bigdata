package it.polito.bigdata.spark.example;

import java.io.Serializable;

@SuppressWarnings("serial")
public class StationTimeslot implements Serializable {
	
	private String station;
	private String timeslot;
	private int entryCount; //sum all the entryCount for a timeslot to obtain the number of reading for that timeslot
	private int entryFreeSlotZeroCount; //sum all values for a timeslot to obtain the total number of reading with free slot equal to zero
	
	public String getStation() {
		return station;
	}

	public void setStation(String stationID) {
		this.station = stationID;
	}

	public String getTimeslot() {
		return timeslot;
	}

	public void setTimeslot(String timeslot) {
		this.timeslot = timeslot;
	}

	public int getEntryCount() {
		return entryCount;
	}

	public void setEntryCount(int entryCount) {
		this.entryCount = entryCount;
	}

	public int getEntryFreeSlotZeroCount() {
		return entryFreeSlotZeroCount;
	}

	public void setEntryFreeSlotZeroCount(int entryFreeSlotZeroCount) {
		this.entryFreeSlotZeroCount = entryFreeSlotZeroCount;
	}

}
