package com.adp.esi.digitech.dvts.processing.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.adp.esi.digitech.dvts.processing.autowire.service.CustomDataProcessorDynamicAutowireService;
import com.adp.esi.digitech.dvts.processing.enums.Status;
import com.adp.esi.digitech.dvts.processing.enums.ValidationType;
import com.adp.esi.digitech.dvts.processing.exception.DataValidationException;
import com.adp.esi.digitech.dvts.processing.exception.ProcessException;
import com.adp.esi.digitech.dvts.processing.exception.TransformationException;
import com.adp.esi.digitech.dvts.processing.model.ApiResponse;
import com.adp.esi.digitech.dvts.processing.model.DataPayload;
import com.adp.esi.digitech.dvts.processing.model.Row;
import com.adp.esi.digitech.dvts.processing.service.DataProcessingService;

import lombok.extern.slf4j.Slf4j;

@RestController
@RequestMapping("/ahub/dvts/process")
@Slf4j
public class DataProcessingController {
	
	@Autowired
	CustomDataProcessorDynamicAutowireService dataProcessingService;

	@PostMapping("/data")
	public ResponseEntity<ApiResponse<List<Row>>> process(@RequestBody DataPayload payload) throws DataValidationException, ProcessException {
		ApiResponse<List<Row>> response = null;
		try {
		
			log.info("DataProcessingController -> process() Received request for data processing, context = {}, datasetId = {}, datasetName = {}, validationType = {}", payload.getRequestContext(), payload.getDatasetId(), payload.getDatasetName(), payload.getValidationType());
		
			var rows = dataProcessingService.process(DataProcessingService.class, payload);
		
			log.info("DataProcessingController -> process() completed request for data processing, context = {}, datasetId = {}, datasetName = {}, validationType = {}, message = {}", payload.getRequestContext(), payload.getDatasetId(), payload.getDatasetName(), payload.getValidationType(), "Data  processed successfully. Passed all validation rules.");
		
			response = ApiResponse.success(Status.SUCCESS, "Data processed successfully", rows);		
		
		} catch (DataValidationException e) {
			log.error("DataProcessingController -> process() Failed with data validations, context = {}, datasetId = {}, datasetName = {}, validationType = {}, message = {}", payload.getRequestContext(), payload.getDatasetId(), payload.getDatasetName(), payload.getValidationType(), "Failed with data validations");
			var status = e.getValidationType().equals(ValidationType.client) ? Status.CLIENT_DATA_VALIDATION : e.getValidationType().equals(ValidationType.CAM) ? Status.CAM_DATA_VALIDATION : Status.FAILED;
			
			response = ApiResponse.error(status, "Failed with data validations, Found Validation erros.", e.getErrorRows());
		} catch (TransformationException | ProcessException e) {
			log.error("DataProcessingController -> process() Data process failed with erros, context = {}, datasetId = {}, datasetName = {}, validationType = {}, message = {}", payload.getRequestContext(), payload.getDatasetId(), payload.getDatasetName(), payload.getValidationType(), e.getMessage());
			response = ApiResponse.error(Status.ERROR, "Data process failed with error " + e.getMessage(), null);
		}
		return ResponseEntity.ok().body(response);
	}
		
}
