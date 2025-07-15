package com.adp.esi.digitech.dvts.processing.validation.util;

import org.json.JSONObject;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;

import com.adp.esi.digitech.dvts.processing.model.Column;
import com.adp.esi.digitech.dvts.processing.model.RequestContext;

public abstract class AbstractValidationUtils<T> implements IValidationUtils<T> {
	
	
	protected ObjectProvider<Column> columnObjProvider;
	
	protected RequestContext requestContext;
	
	public AbstractValidationUtils(RequestContext requestContext) {
		this.requestContext = requestContext;
	}

	@Override
	public JSONObject constructError(String columnName, String columnValue, String errorMessage, int rowNumber) {
		JSONObject error = new JSONObject();
		error.put("ColumnName", columnName);
		error.put("ColumnValue", columnValue);
		error.put("ErrorMessage", errorMessage);			
		error.put("rowNumber", String.valueOf(rowNumber));
		return error;
	}
	
	@Autowired
	public void setColumnObjProvider(ObjectProvider<Column> columnObjProvider) {
		this.columnObjProvider = columnObjProvider;
	}
	
	
	

}
