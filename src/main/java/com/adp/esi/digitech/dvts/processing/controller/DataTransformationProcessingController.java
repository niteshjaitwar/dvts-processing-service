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
import com.adp.esi.digitech.dvts.processing.exception.ProcessException;
import com.adp.esi.digitech.dvts.processing.exception.TransformationException;
import com.adp.esi.digitech.dvts.processing.model.ApiResponse;
import com.adp.esi.digitech.dvts.processing.model.DataPayload;
import com.adp.esi.digitech.dvts.processing.model.Row;
import com.adp.esi.digitech.dvts.processing.service.DataTransformationProcessingService;

import lombok.extern.slf4j.Slf4j;

@RestController
@RequestMapping("/ahub/dvts/transform")
@Slf4j
public class DataTransformationProcessingController {
	
	@Autowired
	CustomDataProcessorDynamicAutowireService dataTransformationProcessingService;
	
	@PostMapping("/data")
	public ResponseEntity<ApiResponse<List<Row>>> process(@RequestBody DataPayload payload) throws TransformationException, ProcessException {
		ApiResponse<List<Row>> response = null;
		try {
			log.info("DataTransformationProcessingController -> process() Received request for data transformation processing, context = {}, datasetId = {}, datasetName = {}, batchName = {}, batchSize = {}", payload.getRequestContext(), payload.getDatasetId(), payload.getDatasetName(), payload.getBatchName(), payload.getBatchSize());
			
			var rows = dataTransformationProcessingService.process(DataTransformationProcessingService.class, payload);
			
			log.info("DataTransformationProcessingController -> process() completed request for data transformation processing, context = {}, datasetId = {}, datasetName = {}, batchName = {}, batchSize = {}, message = {}", payload.getRequestContext(), payload.getDatasetId(), payload.getDatasetName(), payload.getBatchName(), payload.getBatchSize(), "Data transformation completed successfully.");
			
			response = ApiResponse.success(Status.SUCCESS, "Data transformation processed successfully.", rows);
			
			return ResponseEntity.ok().body(response);
		} catch(TransformationException | ProcessException e) {
			log.error("DataTransformationProcessingController -> process() transformation process failed with erros, context = {}, datasetId = {}, datasetName = {}, batchName = {}, batchSize = {}, message = {}", payload.getRequestContext(), payload.getDatasetId(), payload.getDatasetName(), payload.getBatchName(), payload.getBatchSize(), e.getMessage());
			response = ApiResponse.error(Status.ERROR, "Data transformation process failed with error " + e.getMessage(), null);
		}
		
		return ResponseEntity.ok().body(response);
	}

}
