package com.adp.esi.digitech.dvts.processing.validation.util;

import java.util.ArrayList;

import org.json.JSONObject;

import com.adp.esi.digitech.dvts.processing.exception.DataValidationException;
import com.adp.esi.digitech.dvts.processing.model.Column;
import com.adp.esi.digitech.dvts.processing.model.Row;

public interface IValidationUtils<T> {
	
	ArrayList<String> validate(Row row, Column column, T rule) throws DataValidationException;
	
	JSONObject constructError(String columnName, String columnValue, String errorMessage, int rowNumber);

}
