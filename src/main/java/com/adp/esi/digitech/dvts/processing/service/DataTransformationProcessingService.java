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
import com.adp.esi.digitech.dvts.processing.ds.model.ComplexLovData;
import com.adp.esi.digitech.dvts.processing.enums.ProcessType;
import com.adp.esi.digitech.dvts.processing.model.DataPayload;
import com.adp.esi.digitech.dvts.processing.model.DataSet;
import com.adp.esi.digitech.dvts.processing.model.Row;
import com.adp.esi.digitech.dvts.processing.transformation.service.DataTransformService;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class DataTransformationProcessingService extends AbstractDataProcessingService {
	
	@Autowired
	private CustomTransformDynamicAutowireService autowireService;
	
	@Override
	public List<Row> process(DataPayload payload) {
		
		var context = payload.getRequestContext();		
				
		var transformationRules = datastudioConfigurationService.findAllTransformationRulesMapByBatch(context.getBu(), context.getPlatform(), context.getDataCategory());
				
		var lovTypes = getLovTypeNamesFromTransformationRule(transformationRules);		
		
		Map<String, Properties> lovMap = Objects.nonNull(lovTypes) && !lovTypes.isEmpty() ? loadLovTypes(lovTypes) : null;
		
		var complexLovTypes= getComplexLovTypeNamesFromTransformationRule(transformationRules);
		
		Map<String, ComplexLovData> complexLovMap= Objects.nonNull(complexLovTypes) && !complexLovTypes.isEmpty() ? loadComplexLovTypes(complexLovTypes):null;
		
		
		DataSet dataSet = new DataSet();
		dataSet.setId(payload.getDatasetId());
		dataSet.setName(payload.getDatasetName());
		dataSet.setValidationType(payload.getValidationType());	
		var dataMapRows = payload.getData();		
		var rows = getRows(dataMapRows);		
		dataSet.setData(parseDataByTransformationRules(rows, transformationRules));	
		
		applyDataRule(dataSet, payload.getDataSetRule(), ProcessType.chunks, payload.getClause());
			
		return autowireService.transform(DataTransformService.class, dataSet, context, transformationRules, lovMap, complexLovMap);
	}

}
