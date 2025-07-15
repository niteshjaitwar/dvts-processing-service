package com.adp.esi.digitech.dvts.processing.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.regex.Pattern;

public class DateUtil {
	
	
	public static boolean isDateValid(String dateValue, String dateFormat) {
		String regex =null;	
		if(dateValue.trim().equals("")) {
			return true;
		}else {
			long count = dateFormat.toUpperCase().chars().filter(ch -> ch == 'M').count();
			
			switch (String.valueOf(count)) {
				case "2":
					regex= dateFormat.toUpperCase().replace("DD", "\\d{2}").replace("MM", "\\d{2}").replace("YY", "\\d{2}");
					break;
				case "3":
					regex= dateFormat.toUpperCase().replace("DD", "\\d{2}").replace("MMM", "[a-zA-Z]{3}").replace("YY", "\\d{2}");
					break;
				case "5":
					String temp = dateValue.toUpperCase();
					if(!(temp.contains("JANUARY")||temp.contains("FEBRUARY")||temp.contains("MARCH")||temp.contains("APRIL")||temp.contains("MAY")||temp.contains("JUNE")||temp.contains("JULY")
						||temp.contains("AUGUST")||temp.contains("SEPTEMBER")||temp.contains("OCTOBER")||temp.contains("NOVEMBER")||temp.contains("DECEMBER"))) {
						return false;
					}
					regex= dateFormat.toUpperCase().replace("DD", "\\d{2}").replace("MMMMM", "[a-zA-Z]{3,9}").replace("YY", "\\d{2}");
					
					break;
				default:
					break;
			}			
			
			if(Pattern.matches(regex, dateValue)) {    
	                SimpleDateFormat sdfrmt = new SimpleDateFormat(dateFormat);
	                sdfrmt.setLenient(false);            
	                try {
	                    sdfrmt.parse(dateValue); 
	                    return true;
	                }catch (ParseException e) {
	                    return false;
	                }			
			}	
		}
		return false;
	}
}