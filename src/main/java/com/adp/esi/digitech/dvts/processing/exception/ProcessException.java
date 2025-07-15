package com.adp.esi.digitech.dvts.processing.exception;

public class ProcessException extends RuntimeException {
	
	private static final long serialVersionUID = -1875498832366251382L;

	public ProcessException(String message) {
		super(message);
	}
	
	public ProcessException(String message, Throwable cause) {
		super(message, cause);
	}	
}
