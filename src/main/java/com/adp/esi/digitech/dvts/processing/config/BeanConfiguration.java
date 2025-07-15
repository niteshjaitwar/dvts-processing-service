package com.adp.esi.digitech.dvts.processing.config;

import java.text.SimpleDateFormat;
import java.util.TimeZone;
import java.util.UUID;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import com.adp.esi.digitech.dvts.processing.model.Column;
import com.fasterxml.jackson.databind.ObjectMapper;

@Configuration
public class BeanConfiguration {	
	
	@Bean
	public SimpleDateFormat simpleDateFormat() {
		SimpleDateFormat s = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		s.setTimeZone(TimeZone.getTimeZone("GMT"));
		return s;
	}
	
	
	@Bean
	public ObjectMapper objectMapper() {
		return new ObjectMapper();
	}
	
	@Bean
	@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
	public Column column(String name, Object value, UUID uuid, String sourceKey) {
		return new Column(name, value, uuid, sourceKey);
	}
		
}
