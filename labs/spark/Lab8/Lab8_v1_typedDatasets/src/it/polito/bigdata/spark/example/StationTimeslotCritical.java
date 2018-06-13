package it.polito.bigdata.spark.example;

import java.io.Serializable;

public class StationTimeslotCritical implements Serializable{

	private String station;
	private String timeslot;
	private Float criticality;
	
	
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

	public Float getCriticality() {
		return criticality;
	}

	public void setCriticality(Float criticality) {
		this.criticality = criticality;
	}
	
	
}
