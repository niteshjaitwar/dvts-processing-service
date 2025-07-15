package com.adp.esi.digitech.dvts.processing.filter.service;

import java.util.List;
import java.util.Map;

import com.adp.esi.digitech.dvts.processing.ds.config.model.DataSetRules;
import com.adp.esi.digitech.dvts.processing.enums.ProcessType;
import com.adp.esi.digitech.dvts.processing.exception.TransformationException;
import com.adp.esi.digitech.dvts.processing.model.RequestContext;
import com.adp.esi.digitech.dvts.processing.model.Row;

public interface IFilterService {
	
	public List<Row> filter(List<Row> data, DataSetRules rules) throws TransformationException;
	
	public void setRequestContext(RequestContext requestContext);
	
	public void setProcessType(ProcessType processType);
	
	public void setDynamicClauseValues(Map<String, String> dynamicClauseValues);

}
