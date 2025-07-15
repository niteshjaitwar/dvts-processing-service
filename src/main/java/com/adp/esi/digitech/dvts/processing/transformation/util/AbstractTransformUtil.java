package com.adp.esi.digitech.dvts.processing.transformation.util;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;


import com.adp.esi.digitech.dvts.processing.enums.TransformType;
import com.adp.esi.digitech.dvts.processing.model.Column;
import com.adp.esi.digitech.dvts.processing.model.RequestContext;
import com.fasterxml.jackson.databind.ObjectMapper;

public abstract class AbstractTransformUtil implements ITransformUtil {

	RequestContext requestContext;	
	
	TransformType transformationType;
	
	ObjectProvider<Column> columnObjProvider;	
	
	ObjectMapper objectMapper;
	

	public void setRequestContext(RequestContext requestContext) {
		this.requestContext = requestContext;
	}

	public void setTransformationType(TransformType transformationType) {
		this.transformationType = transformationType;
	}
	
	@Autowired
	public void setColumnObjProvider(ObjectProvider<Column> columnObjProvider) {
		this.columnObjProvider = columnObjProvider;
	}
	
	@Autowired
	public void setObjectMapper(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}
	
	
	
}
