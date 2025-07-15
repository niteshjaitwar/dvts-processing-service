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
import com.adp.esi.digitech.dvts.processing.model.ApiResponse;
import com.adp.esi.digitech.dvts.processing.model.DataPayload;
import com.adp.esi.digitech.dvts.processing.model.Row;
import com.adp.esi.digitech.dvts.processing.service.DataValidationProcessingService;

import lombok.extern.slf4j.Slf4j;

@RestController
@RequestMapping("/ahub/dvts/validate")
@Slf4j
public class DataValidationProcessingController {
	
	@Autowired
	CustomDataProcessorDynamicAutowireService dataValidationProcessingService;
	
	@PostMapping("/data")
	public ResponseEntity<ApiResponse<List<Row>>> process(@RequestBody DataPayload payload) throws DataValidationException, ProcessException {
		ApiResponse<List<Row>> response = null;
		try {
		
			log.info("DataValidationProcessingController -> process() Received request for data validation processing, context = {}, datasetId = {}, datasetName = {}, validationType = {}, batchName = {}, batchSize = {}", payload.getRequestContext(), payload.getDatasetId(), payload.getDatasetName(), payload.getValidationType(), payload.getBatchName(), payload.getBatchSize());
		
			dataValidationProcessingService.process(DataValidationProcessingService.class, payload);
		
			log.info("DataValidationProcessingController -> process() completed request for data validation processing, context = {}, datasetId = {}, datasetName = {}, validationType = {}, batchName = {}, batchSize = {}, message = {}", payload.getRequestContext(), payload.getDatasetId(), payload.getDatasetName(), payload.getValidationType(), payload.getBatchName(), payload.getBatchSize(), "Data  processed successfully. Passed all validation rules.");
		
			response = ApiResponse.success(Status.SUCCESS, "Data validations processed successfully. Passed all validation rules.", null);
		
		
		} catch (DataValidationException e) {
			log.error("DataValidationProcessingController -> process() Failed with data validations, context = {}, datasetId = {}, datasetName = {}, validationType = {}, batchName = {}, batchSize = {}, message = {}", payload.getRequestContext(), payload.getDatasetId(), payload.getDatasetName(), payload.getValidationType(), payload.getBatchName(), payload.getBatchSize(), "Failed with data validations");
			var status = e.getValidationType().equals(ValidationType.client) ? Status.CLIENT_DATA_VALIDATION : e.getValidationType().equals(ValidationType.CAM) ? Status.CAM_DATA_VALIDATION : Status.FAILED;
			
			response = ApiResponse.error(status, "Data validations processed successfully. Found Validation erros.", e.getErrorRows());
		} catch (ProcessException e) {
			log.error("DataValidationProcessingController -> process() validation process failed with erros, context = {}, datasetId = {}, datasetName = {}, validationType = {}, batchName = {}, batchSize = {}, message = {}", payload.getRequestContext(), payload.getDatasetId(), payload.getDatasetName(), payload.getValidationType(), payload.getBatchName(), payload.getBatchSize(), e.getMessage());
			
			response = ApiResponse.error(Status.ERROR, "Data validations process failed with error " + e.getMessage(), null);
		}
		
		return ResponseEntity.ok().body(response);
		
	}

}
