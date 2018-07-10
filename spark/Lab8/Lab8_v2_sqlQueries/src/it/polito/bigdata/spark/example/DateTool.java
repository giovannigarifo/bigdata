package it.polito.bigdata.spark.example;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateTool {

	public static String DayOfTheWeek(Timestamp date) {

		String dayOfTheWeek;
		Date d = new Date(date.getTime());
		Calendar cal = Calendar.getInstance();

		cal.setTime(d);

		switch (cal.get(Calendar.DAY_OF_WEEK)) {
		case Calendar.SUNDAY:
			dayOfTheWeek = "Sun";
			break;

		case Calendar.MONDAY:
			dayOfTheWeek = "Mon";
			break;

		case Calendar.TUESDAY:
			dayOfTheWeek = "Tue";
			break;

		case Calendar.WEDNESDAY:
			dayOfTheWeek = "Wed";
			break;

		case Calendar.THURSDAY:
			dayOfTheWeek = "Thu";
			break;

		case Calendar.FRIDAY:
			dayOfTheWeek = "Fri";
			break;

		case Calendar.SATURDAY:
			dayOfTheWeek = "Sat";
			break;

		default:
			dayOfTheWeek = "Sat";

		}

		return dayOfTheWeek;
	}

	public static String DayOfTheWeek(String date) {

		String dayOfTheWeek;
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		Date d = new Date();
		Calendar cal = Calendar.getInstance();

		try {
			d = format.parse(date);
		} catch (ParseException e) {
			e.printStackTrace();
		}

		cal.setTime(d);

		switch (cal.get(Calendar.DAY_OF_WEEK)) {
		case Calendar.SUNDAY:
			dayOfTheWeek = "Sun";
			break;

		case Calendar.MONDAY:
			dayOfTheWeek = "Mon";
			break;

		case Calendar.TUESDAY:
			dayOfTheWeek = "Tue";
			break;

		case Calendar.WEDNESDAY:
			dayOfTheWeek = "Wed";
			break;

		case Calendar.THURSDAY:
			dayOfTheWeek = "Thu";
			break;

		case Calendar.FRIDAY:
			dayOfTheWeek = "Fri";
			break;

		case Calendar.SATURDAY:
			dayOfTheWeek = "Sat";
			break;

		default:
			dayOfTheWeek = "Sat";

		}

		return dayOfTheWeek;
	}

	public static int hour(Timestamp date) {

		Date d = new Date(date.getTime());
		Calendar cal = Calendar.getInstance();

		cal.setTime(d);

		return cal.get(Calendar.HOUR_OF_DAY);
	}

}