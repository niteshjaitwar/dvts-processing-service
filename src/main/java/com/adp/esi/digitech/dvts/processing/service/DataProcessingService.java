package com.adp.esi.digitech.dvts.processing.service;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.adp.esi.digitech.dvts.processing.autowire.service.CustomTransformDynamicAutowireService;
import com.adp.esi.digitech.dvts.processing.autowire.service.CustomValidationDynamicAutowireService;
import com.adp.esi.digitech.dvts.processing.ds.model.ComplexLovData;
import com.adp.esi.digitech.dvts.processing.enums.ProcessType;
import com.adp.esi.digitech.dvts.processing.enums.ValidationType;
import com.adp.esi.digitech.dvts.processing.exception.DataValidationException;
import com.adp.esi.digitech.dvts.processing.exception.ProcessException;
import com.adp.esi.digitech.dvts.processing.exception.TransformationException;
import com.adp.esi.digitech.dvts.processing.model.DataPayload;
import com.adp.esi.digitech.dvts.processing.model.DataSet;
import com.adp.esi.digitech.dvts.processing.model.Row;
import com.adp.esi.digitech.dvts.processing.transformation.service.DataTransformService;
import com.adp.esi.digitech.dvts.processing.validation.service.DataValidationService;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class DataProcessingService extends AbstractDataProcessingService {
	
	@Autowired
	private CustomValidationDynamicAutowireService autowireValidationService;
	
	@Autowired
	private CustomTransformDynamicAutowireService autowireTransformService;


	@Override
	public List<Row> process(DataPayload payload)
			throws DataValidationException, TransformationException, ProcessException {
		
		var validationRules = datastudioConfigurationService.findAllValidationRulesMapByRuleType(requestContext.getBu(), requestContext.getPlatform(), requestContext.getDataCategory(), ValidationType.client);
		var camValidationRules = datastudioConfigurationService.findAllValidationRulesMapByRuleType(requestContext.getBu(), requestContext.getPlatform(), requestContext.getDataCategory(), ValidationType.CAM);
		
		var lovTypes = getLovTypeNamesFromValidationRule(validationRules);		
		Map<String, Properties> lovMap = Objects.nonNull(lovTypes) && !lovTypes.isEmpty() ? loadLovTypes(lovTypes) : null;
		
		var complexLovTypes = getComplexLovTypeNamesFromValidationRule(validationRules);
		Map<String, ComplexLovData> complexLovMap = Objects.nonNull(complexLovTypes) && !complexLovTypes.isEmpty()
				? loadComplexLovTypes(complexLovTypes)
				: null;
		
		//log.info("DataProcessingService - process(), Found complexLovMap Rules: {}", complexLovMap);
		
		DataSet dataSet = new DataSet();
		dataSet.setId(payload.getDatasetId());
		dataSet.setName(payload.getDatasetName());			
		
		var dataMapRows = payload.getData();		
		var rows = getRows(dataMapRows);		
		dataSet.setData(rows);
		
		if (Objects.nonNull(camValidationRules) && !camValidationRules.isEmpty()) {
			log.info("DataProcessingService - process(), Found CAM Rules: {}", camValidationRules.size());
			dataSet.setValidationType(ValidationType.CAM);	
			autowireValidationService.validate(DataValidationService.class, dataSet, requestContext, camValidationRules, lovMap, complexLovMap, payload.getColumnsToValidate());
		}
		
		dataSet.setValidationType(ValidationType.client);		
		autowireValidationService.validate(DataValidationService.class, dataSet, requestContext, validationRules, lovMap, complexLovMap, payload.getColumnsToValidate());		
		
		dataSet.setData(parseDataByValidationRules(dataSet.getData(), validationRules));		
		
		applyDataRule(dataSet, payload.getDataSetRule(), ProcessType.in_memory, payload.getClause());
		
		var transformationRules = datastudioConfigurationService.findAllTransformationRulesMapBy(requestContext.getBu(), requestContext.getPlatform(), requestContext.getDataCategory());
		return autowireTransformService.transform(DataTransformService.class, dataSet, requestContext, transformationRules, lovMap, complexLovMap);
	}

}
