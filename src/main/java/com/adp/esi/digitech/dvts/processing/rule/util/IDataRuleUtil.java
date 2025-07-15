package com.adp.esi.digitech.dvts.processing.rule.util;

import java.util.Map;

import com.adp.esi.digitech.dvts.processing.exception.TransformationException;
import com.adp.esi.digitech.dvts.processing.model.RequestContext;

public interface IDataRuleUtil<T,R> {
	
	public T construct(R rule) throws TransformationException;
	public void setRequestContext(RequestContext requestContext);
	public void setDynamicValues(Map<String, String> dynamicValues);
	public void setDatasetName(String datasetName);

}
