package com.adp.esi.digitech.dvts.processing.ds.dto;

import com.adp.esi.digitech.dvts.processing.enums.Status;

import lombok.Data;

@Data
public class DSResponseDTO<T> {
	
	private Status status;
	private Object error;
	private T data;
	private String message;
}
