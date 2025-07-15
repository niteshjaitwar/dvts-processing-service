package com.adp.esi.digitech.dvts.processing.model;

import com.adp.esi.digitech.dvts.processing.enums.Status;

import lombok.Getter;

@Getter
public final class ApiResponse<T> {
	
	
	private T data;
	private Status status;
	private String message;
	
	private ApiResponse(Status status, T data) {
        this.data = data;
        this.status = status;
    } 
	
	private ApiResponse(Status status, String message, T data) {
        this.data = data;
        this.status = status;
        this.message = message;
    } 
	
	
	
	
    public static <T> ApiResponse<T> success(
            Status status,
            T data) {
        return new ApiResponse<>(status, data);
    }
	
    public static <T> ApiResponse<T> success(
            Status status,
            String message,
            T data) {
        return new ApiResponse<>(status,message, data);
    }
	
	
    public static <T> ApiResponse<T> error(
            Status status,
            String message,
            T data) {
        return new ApiResponse<>(status,message, data);
    }
 
   
}
