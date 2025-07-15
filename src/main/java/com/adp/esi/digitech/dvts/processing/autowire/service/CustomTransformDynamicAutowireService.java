package com.adp.esi.digitech.dvts.processing.autowire.service;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.context.WebApplicationContext;

import com.adp.esi.digitech.dvts.processing.ds.model.ComplexLovData;
import com.adp.esi.digitech.dvts.processing.ds.model.TransformationRule;
import com.adp.esi.digitech.dvts.processing.ds.model.ValidationRule;
import com.adp.esi.digitech.dvts.processing.exception.TransformationException;
import com.adp.esi.digitech.dvts.processing.model.DataSet;
import com.adp.esi.digitech.dvts.processing.model.RequestContext;
import com.adp.esi.digitech.dvts.processing.model.Row;
import com.adp.esi.digitech.dvts.processing.transformation.service.ITransformService;

@Service("customTransformDynamicAutowireService")
public class CustomTransformDynamicAutowireService {
	
private final WebApplicationContext webApplicationContext;
	
	@Autowired
	public CustomTransformDynamicAutowireService(WebApplicationContext webApplicationContext) {
		this.webApplicationContext = webApplicationContext;
	}
	
	public <T extends ITransformService> List<Row> transform(Class<T> type,DataSet dataSet,RequestContext requestContext, Map<UUID, TransformationRule> transformationRules, Map<String, Properties> lovMetadataMap, Map<String, ComplexLovData> complexLovMetadatMap) throws TransformationException {
		ITransformService transformService = webApplicationContext.getBean(type, requestContext, transformationRules, lovMetadataMap, complexLovMetadatMap);
		return transformService.transform(dataSet);
	}

}
