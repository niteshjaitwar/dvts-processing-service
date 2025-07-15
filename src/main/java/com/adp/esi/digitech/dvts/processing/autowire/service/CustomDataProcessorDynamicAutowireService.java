package com.adp.esi.digitech.dvts.processing.autowire.service;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.context.WebApplicationContext;

import com.adp.esi.digitech.dvts.processing.exception.DataValidationException;
import com.adp.esi.digitech.dvts.processing.exception.ProcessException;
import com.adp.esi.digitech.dvts.processing.exception.TransformationException;
import com.adp.esi.digitech.dvts.processing.model.DataPayload;
import com.adp.esi.digitech.dvts.processing.model.Row;
import com.adp.esi.digitech.dvts.processing.service.IDataProcessingService;


@Service("customProcessorDynamicAutowireService")
public class CustomDataProcessorDynamicAutowireService {
	
	private final WebApplicationContext webApplicationContext;	
	
	@Autowired
	public CustomDataProcessorDynamicAutowireService(WebApplicationContext webApplicationContext) {
		this.webApplicationContext = webApplicationContext;
	}
	
	public <T extends IDataProcessingService> List<Row> process(Class<T> type, DataPayload data) throws  DataValidationException, TransformationException ,ProcessException {
		IDataProcessingService processorService = webApplicationContext.getBean(type);
		processorService.setRequestContext(data.getRequestContext());
		return processorService.process(data);
	}

}
