package com.adp.esi.digitech.dvts.processing.autowire.service;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collector;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.context.WebApplicationContext;

import com.adp.esi.digitech.dvts.processing.ds.config.model.DataFilter;
import com.adp.esi.digitech.dvts.processing.ds.config.model.DataGroupBy;
import com.adp.esi.digitech.dvts.processing.exception.TransformationException;
import com.adp.esi.digitech.dvts.processing.model.RequestContext;
import com.adp.esi.digitech.dvts.processing.model.Row;
import com.adp.esi.digitech.dvts.processing.rule.util.DataGroupbyRuleUtil;
import com.adp.esi.digitech.dvts.processing.rule.util.IDataRuleUtil;

@Service("customDataRuleUtilDynamicAutowireService")
public class CustomDataRuleUtilDynamicAutowireService {
	
	private final WebApplicationContext webApplicationContext;
	
	@Autowired
	public CustomDataRuleUtilDynamicAutowireService(WebApplicationContext webApplicationContext) {
		this.webApplicationContext = webApplicationContext;
	}
	
	public <T extends IDataRuleUtil<Predicate<Row>, DataFilter>> Predicate<Row> constructFilter(Class<T> type, DataFilter rule, Map<String, String> dynamicValues, String datasetName ,RequestContext requestContext) throws TransformationException {
		IDataRuleUtil<Predicate<Row>, DataFilter> dataFilterRuleUtilService = webApplicationContext.getBean(type);
		dataFilterRuleUtilService.setRequestContext(requestContext);
		dataFilterRuleUtilService.setDynamicValues(dynamicValues);
		dataFilterRuleUtilService.setDatasetName(datasetName);
		return dataFilterRuleUtilService.construct(rule);
	}
	
	public  Collector<Row,?,List<Row>> constructGroupBy(DataGroupBy rule, Map<String, String> dynamicValues, String datasetName, RequestContext requestContext) throws TransformationException {
		DataGroupbyRuleUtil dataGroupByRuleUtilService = webApplicationContext.getBean(DataGroupbyRuleUtil.class);
		dataGroupByRuleUtilService.setRequestContext(requestContext);
		dataGroupByRuleUtilService.setDynamicValues(dynamicValues);
		dataGroupByRuleUtilService.setDatasetName(datasetName);
		return dataGroupByRuleUtilService.construct(rule);
	}
}
