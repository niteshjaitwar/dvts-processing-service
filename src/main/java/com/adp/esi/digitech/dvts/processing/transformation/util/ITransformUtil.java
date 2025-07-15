package com.adp.esi.digitech.dvts.processing.transformation.util;

import org.json.JSONObject;

import com.adp.esi.digitech.dvts.processing.enums.TransformType;
import com.adp.esi.digitech.dvts.processing.exception.TransformationException;
import com.adp.esi.digitech.dvts.processing.model.Column;
import com.adp.esi.digitech.dvts.processing.model.RequestContext;
import com.adp.esi.digitech.dvts.processing.model.Row;

public interface ITransformUtil {	
	
	public void setRequestContext(RequestContext requestContext);
	
	public void setTransformationType(TransformType transformationType);
	
	void transform(Row row, Column column, JSONObject rule) throws TransformationException;

}
