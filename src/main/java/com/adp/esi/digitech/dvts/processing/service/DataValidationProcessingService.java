package com.adp.esi.digitech.dvts.processing.service;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.adp.esi.digitech.dvts.processing.autowire.service.CustomValidationDynamicAutowireService;
import com.adp.esi.digitech.dvts.processing.ds.model.ComplexLovData;
import com.adp.esi.digitech.dvts.processing.exception.DataValidationException;
import com.adp.esi.digitech.dvts.processing.exception.ProcessException;
import com.adp.esi.digitech.dvts.processing.model.DataPayload;
import com.adp.esi.digitech.dvts.processing.model.DataSet;
import com.adp.esi.digitech.dvts.processing.model.Row;
import com.adp.esi.digitech.dvts.processing.validation.service.DataValidationService;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class DataValidationProcessingService extends AbstractDataProcessingService {
	
	@Autowired
	private CustomValidationDynamicAutowireService autowireService;
	
	@Override
	public List<Row> process(DataPayload payload) throws DataValidationException, ProcessException{
		
		var context = payload.getRequestContext();		
		
		var validationRules = datastudioConfigurationService.findAllValidationRulesMapByRuleTypeBatch(context.getBu(), context.getPlatform(), context.getDataCategory(), payload.getValidationType());
			
		var lovTypes = getLovTypeNamesFromValidationRule(validationRules);		
		Map<String, Properties> lovMap = Objects.nonNull(lovTypes) && !lovTypes.isEmpty() ? loadLovTypes(lovTypes) : null;
		
		var complexLovTypes = getComplexLovTypeNamesFromValidationRule(validationRules);
		Map<String, ComplexLovData> complexLovMap = Objects.nonNull(complexLovTypes) && !complexLovTypes.isEmpty()
				? loadComplexLovTypes(complexLovTypes)
				: null;
		
		DataSet dataSet = new DataSet();
		dataSet.setId(payload.getDatasetId());
		dataSet.setName(payload.getDatasetName());
		dataSet.setValidationType(payload.getValidationType());		
		var dataMapRows = payload.getData();		
		var rows = getRows(dataMapRows);		
		dataSet.setData(rows);				
		
		autowireService.validate(DataValidationService.class, dataSet, context, validationRules, lovMap, complexLovMap, payload.getColumnsToValidate());		
		
		return null;
	}

}
