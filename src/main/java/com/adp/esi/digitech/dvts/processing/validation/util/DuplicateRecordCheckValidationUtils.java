package com.adp.esi.digitech.dvts.processing.validation.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.adp.esi.digitech.dvts.processing.ds.model.ValidationRule;
import com.adp.esi.digitech.dvts.processing.exception.ProcessException;
import com.adp.esi.digitech.dvts.processing.model.Column;
import com.adp.esi.digitech.dvts.processing.model.RequestContext;
import com.adp.esi.digitech.dvts.processing.model.Row;
import com.adp.esi.digitech.dvts.processing.util.ValidationUtil;

import lombok.extern.slf4j.Slf4j;

@Service("duplicateRecordCheckValidationUtils")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class DuplicateRecordCheckValidationUtils extends AbstractValidationUtils<Map<String, ValidationRule>> {
	
	private String dataSetName;	
	
	@Autowired
	public DuplicateRecordCheckValidationUtils(RequestContext requestContext, String dataSetName) {
		super(requestContext);
		this.dataSetName = dataSetName;
	}

	public void validate(List<Row> rows, Map<UUID, ValidationRule> rules) throws ProcessException {
		try {
			log.info("DuplicateRecordCheckValidationUtils - validate(), Starting duplicate rows and columns validations requestUuid = {}, dataSetName = {}", requestContext.getRequestUuid(), dataSetName);
				List<UUID> fields = rules.entrySet().parallelStream().map(entrySet -> {
					ValidationRule rule = entrySet.getValue();
					if (rule != null && ValidationUtil.isHavingValue(rule.getUniqueValueInColumn())
							&& rule.getUniqueValueInColumn().equalsIgnoreCase("Y")) {
						return entrySet.getKey();
					}
					return null;
				}).filter(Objects::nonNull).collect(Collectors.toList());
					
				log.info("DuplicateRecordCheckValidationUtils - validate() found fields,  requestUuid = {}, dataSetName = {}, fields = {}", requestContext.getRequestUuid(), dataSetName, fields);
					
				if (fields != null && !fields.isEmpty()) {
					if (fields.size() > 1) {
						List<String> finalList = rows.parallelStream().map(row -> {							
							return fields.stream().map(field -> (String) row.getColumns().get(field).getValue()).collect(Collectors.joining());
						}).collect(Collectors.toList());
		
						finalList.stream().filter(data -> Collections.frequency(finalList, data) > 1).forEach(duplicateRow -> {
							int index = finalList.indexOf(duplicateRow);
							fields.forEach(field -> {
								if(rows.get(index).getColumns().get(field).getErrors() == null) {
									rows.get(index).getColumns().get(field).setErrors(new ArrayList<String>());
								}
								rows.get(index).getColumns().get(field).getErrors().add("Duplicate record found");
								
							});
		
						});
					} else if (fields.size() == 1) {
						List<String> finalList = rows.parallelStream()
								.map(row -> (String) row.getColumns().get(fields.get(0)).getValue())
								.filter(ValidationUtil::isHavingValue).collect(Collectors.toList());
		
						finalList.stream().filter(data -> Collections.frequency(finalList, data) > 1).forEach(duplicateRow -> {
							int index = finalList.indexOf(duplicateRow);
							if(rows.get(index).getColumns().get(fields.get(0)).getErrors() == null) {
								rows.get(index).getColumns().get(fields.get(0)).setErrors(new ArrayList<String>());
							}
							rows.get(index).getColumns().get(fields.get(0)).getErrors().add("Duplicate record found");
						});
					}
					
				}
				log.info("DuplicateRecordCheckValidationUtils - validate(), Completed duplicate rows and columns validations requestUuid = {}, dataSetName = {}", requestContext.getRequestUuid(), dataSetName);
				
		} catch(Exception e) {
			log.error("DuplicateRecordCheckValidationUtils - validate(), Failed duplicate rows and columns data validations requestUuid = {}, message = {}", requestContext.getRequestUuid(), e.getMessage());
			throw new ProcessException("Data Validation Error -  " + e.getMessage(), e.getCause());
		}
		
	}

	@Override
	public ArrayList<String> validate(Row row, Column column, Map<String, ValidationRule> rule) {
		return null;
	}
}
