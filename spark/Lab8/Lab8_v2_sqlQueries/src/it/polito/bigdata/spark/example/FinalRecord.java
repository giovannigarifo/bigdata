package it.polito.bigdata.spark.example;

import java.io.Serializable;

public class FinalRecord implements Serializable {

	private String station;
	private String day;
	private int hour;
	private Double longitude;
	private Double latitude;
	private Float criticality;
	
	public String getStation() {
		return station;
	}
	public void setStation(String station) {
		this.station = station;
	}
	public String getDay() {
		return day;
	}
	public void setDay(String day) {
		this.day = day;
	}
	public Double getLongitude() {
		return longitude;
	}
	public void setLongitude(Double longitude) {
		this.longitude = longitude;
	}
	public int getHour() {
		return hour;
	}
	public void setHour(int hour) {
		this.hour = hour;
	}
	public Float getCriticality() {
		return criticality;
	}
	public void setCriticality(Float criticality) {
		this.criticality = criticality;
	}
	public Double getLatitude() {
		return latitude;
	}
	public void setLatitude(Double latitude) {
		this.latitude = latitude;
	}
	

}
