package com.adp.esi.digitech.dvts.processing;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import com.adp.esi.digitech.dvts.processing.ds.service.DatastudioConfigurationService;
import com.adp.esi.digitech.dvts.processing.util.ValidationUtil;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class LargeLOVStartUpRunner implements ApplicationListener<ApplicationReadyEvent> {

	@Autowired
	DatastudioConfigurationService datastudioConfigurationService;
	
	@Value("${large.lov.types}")
	List<String> largeLovTypes;
	
	@Value("${large.lov.file.path}")
	String folderPath;
	
	
	@Override
	public void onApplicationEvent(ApplicationReadyEvent event) {
		
		try {		
			var folder = new File(folderPath);
			if(!folder.exists())
				folder.mkdirs();
	
			if(Objects.isNull(largeLovTypes) || largeLovTypes.isEmpty())
				return;
			
			largeLovTypes.stream().filter(lovType -> ValidationUtil.isHavingValue(lovType)).forEach(lovType -> {
				log.info("LargeLOVStartUpRunner - run() Started retrieve lov type = {}", lovType);
				var props = datastudioConfigurationService.findAllProperties(lovType);
				var finalPath = folderPath + lovType + ".properties";			
				var file = new File(finalPath);
				
				try(OutputStream out = new FileOutputStream(file); Writer writer = new OutputStreamWriter(out, StandardCharsets.UTF_16)) {
					if(!file.exists())
						file.createNewFile();				
					props.store(writer, lovType);
				} catch (Exception e) {
					log.error("LargeLOVStartUpRunner - run() Failed to retrieve lov type = {}, message = {}", lovType, e.getMessage());
				}
				log.info("LargeLOVStartUpRunner - run() Completed retrieve lov type = {}", lovType);
			});
		} catch(Exception e) {
			log.error("LargeLOVStartUpRunner - run() Initial Fail to retrieve lov types = {}, message = {}", largeLovTypes, e.getMessage());
		}
	}

}
