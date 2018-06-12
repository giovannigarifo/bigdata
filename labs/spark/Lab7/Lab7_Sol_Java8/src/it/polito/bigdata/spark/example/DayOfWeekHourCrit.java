package it.polito.bigdata.spark.example;

import java.io.Serializable;

@SuppressWarnings("serial")
public class DayOfWeekHourCrit implements Serializable {
	public String dayOfTheWeek;
	public int hour;
	public Double criticality;

	DayOfWeekHourCrit(String dayOfTheWeek, int hour, Double criticality) {
		this.dayOfTheWeek = dayOfTheWeek;
		this.hour = hour;
		this.criticality = criticality;
	}

	public String toString() {
		return dayOfTheWeek + "- " + hour + "-" + criticality;
	}
}
