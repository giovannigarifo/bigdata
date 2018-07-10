package it.polito.bigdata.spark.example;

import java.io.Serializable;

@SuppressWarnings("serial")
public class StationLocation implements Serializable {

	private String id;
	private Float longitude;
	private Float latitude;
	
	public String getStation() {
		return id;
	}

	public void setStation(String station) {
		this.id = station;
	}

	public Float getLongitude() {
		return longitude;
	}

	public void setLongitude(Float longitude) {
		this.longitude = longitude;
	}

	public Float getLatitude() {
		return latitude;
	}

	public void setLatitude(Float latitude) {
		this.latitude = latitude;
	}

}