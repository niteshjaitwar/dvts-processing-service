package com.adp.esi.digitech.dvts.processing.service;

import java.util.List;

import com.adp.esi.digitech.dvts.processing.exception.DataValidationException;
import com.adp.esi.digitech.dvts.processing.exception.ProcessException;
import com.adp.esi.digitech.dvts.processing.exception.TransformationException;
import com.adp.esi.digitech.dvts.processing.model.DataPayload;
import com.adp.esi.digitech.dvts.processing.model.RequestContext;
import com.adp.esi.digitech.dvts.processing.model.Row;

public interface IDataProcessingService {
	
	public List<Row> process(DataPayload payload) throws DataValidationException, TransformationException ,ProcessException;
	
	public void setRequestContext(RequestContext requestContext);
}
