package com.adp.esi.digitech.dvts.processing.parser;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.adp.esi.digitech.dvts.processing.util.ValidationUtil;

public class DateParser {

	public static Date parse(String value, String format) throws ParseException {
		if(!ValidationUtil.isHavingValue(value))
			return null;
		SimpleDateFormat sourceDateFormat = new SimpleDateFormat(format);
		return sourceDateFormat.parse(value);
	}

}
