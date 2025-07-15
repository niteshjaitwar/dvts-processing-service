package com.adp.esi.digitech.dvts.processing.autowire.service;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.context.WebApplicationContext;

import com.adp.esi.digitech.dvts.processing.ds.model.ComplexLovData;
import com.adp.esi.digitech.dvts.processing.ds.model.ValidationRule;
import com.adp.esi.digitech.dvts.processing.exception.DataValidationException;
import com.adp.esi.digitech.dvts.processing.model.RequestContext;
import com.adp.esi.digitech.dvts.processing.validation.service.IValidationService;

@Service("customValidatorDynamicAutowireService")
public class CustomValidationDynamicAutowireService {
	
	private final WebApplicationContext webApplicationContext;
	
	@Autowired
	public CustomValidationDynamicAutowireService(WebApplicationContext webApplicationContext) {
		this.webApplicationContext = webApplicationContext;
	}
	
	
	public <T extends IValidationService<V>,V> void validate(Class<T> type, V data, RequestContext requestContext,Map<UUID, ValidationRule> rules, Map<String, Properties> lovMetadataMap, Map<String, ComplexLovData> complexLovMetadataMap, List<String> columnsToValidate) throws DataValidationException {
		IValidationService<V> validatorService = webApplicationContext.getBean(type, requestContext, rules, lovMetadataMap, complexLovMetadataMap, columnsToValidate);		
		validatorService.validate(data);
	}
}
