package com.adp.esi.digitech.dvts.processing.transformation.service;

import java.util.Map;
import java.util.Properties;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;

import com.adp.esi.digitech.dvts.processing.ds.model.ComplexLovData;
import com.adp.esi.digitech.dvts.processing.model.Column;
import com.adp.esi.digitech.dvts.processing.model.RequestContext;

public abstract class AbstractTransformService implements ITransformService {
	
	protected RequestContext requestContext;	
	protected Map<String, Properties> lovMetadataMap;
	protected Map<String, ComplexLovData> complexLovMetadatMap;
	
	public AbstractTransformService(RequestContext requestContext, Map<String, Properties> lovMetadataMap, Map<String, ComplexLovData> complexLovMetadatMap) {
		this.requestContext = requestContext;
		this.lovMetadataMap = lovMetadataMap;
		this.complexLovMetadatMap = complexLovMetadatMap;
		
	}

	
	public ObjectProvider<Column> columnObjProvider;
	
	@Autowired
	public void setColumnObjProvider(ObjectProvider<Column> columnObjProvider) {
		this.columnObjProvider = columnObjProvider;
	}
}
