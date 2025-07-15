package com.adp.esi.digitech.dvts.processing;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties
public class AhubDVTSProcessingServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(AhubDVTSProcessingServiceApplication.class, args);
	}

}
