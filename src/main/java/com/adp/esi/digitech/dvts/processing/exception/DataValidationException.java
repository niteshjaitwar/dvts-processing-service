package com.adp.esi.digitech.dvts.processing.exception;

import java.util.List;

import com.adp.esi.digitech.dvts.processing.enums.ValidationType;
import com.adp.esi.digitech.dvts.processing.model.Row;

public class DataValidationException extends RuntimeException  {

	private static final long serialVersionUID = -6532587457348252299L;
	
	private List<Row> errorRows;
	
	private ValidationType validationType;
	
	public DataValidationException() {
		super();
	}
	
	public DataValidationException(String message) {
		super(message);
	}
	
	public DataValidationException(String message, Throwable cause) {
		super(message, cause);
	}
	
	public DataValidationException(Throwable cause) {
		super(cause);
	}
	
	public DataValidationException(String message,ValidationType validationType, List<Row> errorRows) {
		super(message);
		this.validationType = validationType;
		this.errorRows = errorRows;		
	}

	public List<Row> getErrorRows() {
		return errorRows;
	}

	public ValidationType getValidationType() {
		return validationType;
	}

	public void setValidationType(ValidationType validationType) {
		this.validationType = validationType;
	}
}
