package com.adp.esi.digitech.dvts.processing.filter.service;

import java.util.Map;

import com.adp.esi.digitech.dvts.processing.enums.ProcessType;
import com.adp.esi.digitech.dvts.processing.model.RequestContext;

public abstract class AbstractFilterService implements IFilterService {
	
	RequestContext requestContext;	
	
	ProcessType processType;
	
	Map<String, String> dynamicClauseValues;
	
	public void setRequestContext(RequestContext requestContext) {
		this.requestContext = requestContext;
	}

	public void setProcessType(ProcessType processType) {
		this.processType = processType;
	}

	public void setDynamicClauseValues(Map<String, String> dynamicClauseValues) {
		this.dynamicClauseValues = dynamicClauseValues;
	}
	
	
	
	

}
