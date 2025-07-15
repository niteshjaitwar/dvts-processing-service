package com.adp.esi.digitech.dvts.processing.validation.service;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.adp.esi.digitech.dvts.processing.ds.model.ComplexLovData;
import com.adp.esi.digitech.dvts.processing.ds.model.ValidationRule;
import com.adp.esi.digitech.dvts.processing.enums.ValidationType;
import com.adp.esi.digitech.dvts.processing.exception.DataValidationException;
import com.adp.esi.digitech.dvts.processing.exception.ProcessException;
import com.adp.esi.digitech.dvts.processing.model.DataSet;
import com.adp.esi.digitech.dvts.processing.model.RequestContext;
import com.adp.esi.digitech.dvts.processing.model.Row;
import com.adp.esi.digitech.dvts.processing.util.ValidationUtil;

import lombok.extern.slf4j.Slf4j;

@Service("dataValidatorService")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class DataValidationService extends AbstractValidationService<DataSet> {	
	
	private Map<UUID, ValidationRule> validationRulesMap;	

	@Autowired
	public DataValidationService(RequestContext requestContext, Map<UUID, ValidationRule> validationRulesMap, Map<String, Properties> lovMetadataMap, Map<String, ComplexLovData> complexLovMetadataMap, List<String> columnsToValidate) {
		super(requestContext, lovMetadataMap, complexLovMetadataMap, columnsToValidate);
		this.validationRulesMap = validationRulesMap;		
	}

	@Override
	public void validate(DataSet dataSet) throws DataValidationException, ProcessException {	
		
		var validationType = dataSet.getValidationType();
		try {		
			
			//log.info("DataValidatorService - validate()  Started validating dataSet, UniqueId {}, name = {}, type = {}", requestContext.getUniqueId(), dataSet.getName(), validationType);
			
			this.globalDataSetUuid = UUID.fromString(dataSet.getId());		
					
			List<Row> rows = dataSet.getData();
			
			if(ValidationType.client.equals(validationType)) {
				var tempRow = rows.get(0);
				var tempColumns = tempRow.getColumns();
				var validationRules = validationRulesMap.entrySet().parallelStream().filter(ruleEntity -> tempColumns.containsKey(ruleEntity.getKey())).map(ruleEntity -> {
					ValidationRule rule = ruleEntity.getValue();
					if (rule != null && ValidationUtil.isHavingValue(rule.getUniqueValueInColumn())
							&& rule.getUniqueValueInColumn().equalsIgnoreCase("Y")) {
						return ruleEntity.getValue();
					}
					return null;
				}).filter(Objects::nonNull).collect(Collectors.toMap(rule -> UUID.fromString(rule.getSourceColumn()), Function.identity()));
				
				if(validationRules != null && !validationRules.isEmpty()) {					
					customValidationUtilDynamicAutowireService.validate(rows, validationRules, requestContext, dataSet.getName());
				}
			}
			
			List<Row> errorRows = rows.parallelStream().map(row -> {
				return validate(row, validationRulesMap);
			}).filter(Objects::nonNull).collect(Collectors.toList());
					
			if(!errorRows.isEmpty()) {
				log.error("DataValidatorService -> validate -> Validaitons found for uniqueId = {}, name = {}, type = {}",requestContext.getUniqueId(), dataSet.getName(), validationType);
				
				var dataValidationException = new DataValidationException("Client - Failed to validate data, Please refer to expection file", validationType, errorRows);
				throw dataValidationException;
			}
			
			
			//log.info("DataValidatorService - validate()  Completed validating dataSet, UniqueId {}, name = {}, type = {}", requestContext.getUniqueId(), dataSet.getName(), validationType);
		} catch(DataValidationException e) {
			log.error("DataValidatorService - validate()  Failed validating dataSet, UniqueId {}, name = {}, type = {}, message = {}", requestContext.getUniqueId(), dataSet.getName(), validationType, e.getMessage());
			throw e;
		} catch(ProcessException e) {
			log.error("DataValidatorService - validate()  Failed with process erros while validating dataSet, UniqueId {}, name = {}, type = {}, message = {}", requestContext.getUniqueId(), dataSet.getName(), validationType, e.getMessage());
			throw e;
		} catch(Exception e) {
			log.error("DataValidatorService - validate()  Failed with erros while validating dataSet, UniqueId {}, name = {}, type = {}, message = {}", requestContext.getUniqueId(), dataSet.getName(), validationType, e.getMessage());
			var processException = new ProcessException("Validation Error - " + e.getMessage(), e.getCause());
			throw processException;
		}
	}	
}
