package com.adp.esi.digitech.dvts.processing.autowire.service;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.context.WebApplicationContext;

import com.adp.esi.digitech.dvts.processing.ds.config.model.DataSetRules;
import com.adp.esi.digitech.dvts.processing.enums.ProcessType;
import com.adp.esi.digitech.dvts.processing.exception.TransformationException;
import com.adp.esi.digitech.dvts.processing.filter.service.IFilterService;
import com.adp.esi.digitech.dvts.processing.model.RequestContext;
import com.adp.esi.digitech.dvts.processing.model.Row;


@Service("customFilterDynamicAutowireService")
public class CustomFilterDynamicAutowireService {

	private final WebApplicationContext webApplicationContext;
	
	@Autowired
	public CustomFilterDynamicAutowireService(WebApplicationContext webApplicationContext) {
		this.webApplicationContext = webApplicationContext;
	}
	
	public <T extends IFilterService> List<Row> filter(Class<T> type, List<Row> data, DataSetRules rules, RequestContext requestContext, ProcessType processType, Map<String, String> dynamicClauseValues) throws TransformationException {
		IFilterService filterService = webApplicationContext.getBean(type);
		filterService.setRequestContext(requestContext);
		filterService.setProcessType(processType);
		filterService.setDynamicClauseValues(dynamicClauseValues);
		return filterService.filter(data, rules);
	}
}
