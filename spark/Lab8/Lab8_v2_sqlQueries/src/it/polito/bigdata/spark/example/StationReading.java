package it.polito.bigdata.spark.example;

import java.io.Serializable;

public class StationReading implements Serializable {

	private String station;
	private String timestamp;
	private int used_slots;
	private int free_slots;
	
	public String getStation() {
		return station;
	}

	public void setStation(String stationID) {
		this.station = stationID;
	}

	public String getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}

	public int getUsed_slots() {
		return used_slots;
	}

	public void setUsed_slots(int used_slots) {
		this.used_slots = used_slots;
	}

	public int getFree_slots() {
		return free_slots;
	}

	public void setFree_slots(int free_slots) {
		this.free_slots = free_slots;
	}

	
}
