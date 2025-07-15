package com.adp.esi.digitech.dvts.processing.validation.service;

import java.util.Map;
import java.util.UUID;

import com.adp.esi.digitech.dvts.processing.ds.model.ValidationRule;
import com.adp.esi.digitech.dvts.processing.exception.DataValidationException;
import com.adp.esi.digitech.dvts.processing.exception.ProcessException;
import com.adp.esi.digitech.dvts.processing.model.Column;
import com.adp.esi.digitech.dvts.processing.model.Row;

public interface IValidationService<T>  {
	
	public void validate(T data) throws DataValidationException, ProcessException;
	
	public Row validate(Row row, Map<UUID, ValidationRule> rules) throws DataValidationException;
	
	public void validate(Row row, Column column, ValidationRule validationRule) throws DataValidationException;

}
