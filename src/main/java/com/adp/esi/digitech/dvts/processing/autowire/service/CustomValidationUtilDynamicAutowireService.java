package com.adp.esi.digitech.dvts.processing.autowire.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.context.WebApplicationContext;

import com.adp.esi.digitech.dvts.processing.ds.model.ValidationRule;
import com.adp.esi.digitech.dvts.processing.exception.DataValidationException;
import com.adp.esi.digitech.dvts.processing.model.Column;
import com.adp.esi.digitech.dvts.processing.model.RequestContext;
import com.adp.esi.digitech.dvts.processing.model.Row;
import com.adp.esi.digitech.dvts.processing.validation.util.DataValidationUtils;
import com.adp.esi.digitech.dvts.processing.validation.util.DuplicateRecordCheckValidationUtils;

@Service("customValidatorUtilDynamicAutowireService")
public class CustomValidationUtilDynamicAutowireService {
	
private final WebApplicationContext webApplicationContext;
	
	@Autowired
	public CustomValidationUtilDynamicAutowireService(WebApplicationContext webApplicationContext) {
		this.webApplicationContext = webApplicationContext;
	}
	
	public ArrayList<String> validate(Row row, Column column, ValidationRule rule, RequestContext requestContext, Object lovProperties)  throws DataValidationException {
		DataValidationUtils dataValidationUtils = webApplicationContext.getBean(DataValidationUtils.class, requestContext, lovProperties);
		return dataValidationUtils.validate(row, column, rule);
	}
	
	public void validate(List<Row> rows, Map<UUID, ValidationRule> rules, RequestContext requestContext, String dataSetName)  throws DataValidationException {
		DuplicateRecordCheckValidationUtils duplicateRecordCheckValidationUtils = webApplicationContext.getBean(DuplicateRecordCheckValidationUtils.class, requestContext, dataSetName);
		duplicateRecordCheckValidationUtils.validate(rows, rules);
	}
	
}
