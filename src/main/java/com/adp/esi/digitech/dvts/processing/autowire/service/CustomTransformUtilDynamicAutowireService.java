package com.adp.esi.digitech.dvts.processing.autowire.service;

import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.context.WebApplicationContext;

import com.adp.esi.digitech.dvts.processing.enums.TransformType;
import com.adp.esi.digitech.dvts.processing.exception.TransformationException;
import com.adp.esi.digitech.dvts.processing.model.Column;
import com.adp.esi.digitech.dvts.processing.model.RequestContext;
import com.adp.esi.digitech.dvts.processing.model.Row;
import com.adp.esi.digitech.dvts.processing.transformation.util.ITransformUtil;

@Service("customTransformUtilDynamicAutowireService")
public class CustomTransformUtilDynamicAutowireService {
	
	private final WebApplicationContext webApplicationContext;
	
	@Autowired
	public CustomTransformUtilDynamicAutowireService(WebApplicationContext webApplicationContext) {
		this.webApplicationContext = webApplicationContext;
	}
	
	
	public <T extends ITransformUtil> void transform(Class<T> type, Row row, Column column, JSONObject rule, RequestContext requestContext, TransformType transformationType) throws TransformationException {
		ITransformUtil transformUtil = webApplicationContext.getBean(type);
		transformUtil.setRequestContext(requestContext);
		transformUtil.setTransformationType(transformationType);
		transformUtil.transform(row, column, rule);
	}

}
