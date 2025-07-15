package com.adp.esi.digitech.dvts.processing.parser;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.ParseException;

public class DecimalParser {

	public static Number parse(String value, String format) throws ParseException {

		DecimalFormat df = new DecimalFormat();
		DecimalFormatSymbols symbols = new DecimalFormatSymbols();
		switch (format.toLowerCase()) {
		case "xx xxx,xx":
			symbols.setDecimalSeparator(',');
			symbols.setGroupingSeparator(' ');
			break;
		case "xx.xxx,xx":
			symbols.setDecimalSeparator(',');
			symbols.setGroupingSeparator('.');
			break;

		case "xx,xxx.xx":
			symbols.setDecimalSeparator('.');
			symbols.setGroupingSeparator(',');
			break;

		case "xx'xxx,xx":
			symbols.setDecimalSeparator(',');
			symbols.setGroupingSeparator('\'');
			break;
		case "xxxxx.xx":
			symbols.setDecimalSeparator('.');
			break;
		case "xxxxx,xx":
			symbols.setDecimalSeparator(',');
			break;
		default:
			break;
		}

		df.setDecimalFormatSymbols(symbols);
		return df.parse(value);

	}

}
