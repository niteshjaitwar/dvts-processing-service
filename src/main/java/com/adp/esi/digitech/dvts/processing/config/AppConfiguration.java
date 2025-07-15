package com.adp.esi.digitech.dvts.processing.config;

import java.util.concurrent.Executor;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.adp.esi.digitech.dvts.processing.request.filter.GzipRequestFilter;

@Configuration
public class AppConfiguration {
	
	@Value("${app.daemon-tasks.thread-core-pool-size:20}")
	private int daemonTasksThreadCorePoolSize;
	
	@Value("${app.daemon-tasks.thread-max-pool-size:50}")
	private int daemonTasksThreadMaxPoolSize;
	
	@Value("${app.daemon-tasks.thread-queue-capacity:5000}")
	private int daemonTasksThreadQueueCapacity;
	
	@Bean
	public Executor asyncExecutor () {
		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		executor.setCorePoolSize(daemonTasksThreadCorePoolSize);
		executor.setMaxPoolSize(daemonTasksThreadMaxPoolSize);
	    executor.setQueueCapacity(daemonTasksThreadQueueCapacity);
	    executor.setThreadNamePrefix("DVTSThread-");
	    executor.initialize();
		return executor;
	}
	
	
	@Bean
	public FilterRegistrationBean<GzipRequestFilter> gzipRequestFilter() {
		FilterRegistrationBean<GzipRequestFilter> registrationBean = new FilterRegistrationBean<>();
		registrationBean.setFilter(new GzipRequestFilter());
		registrationBean.addUrlPatterns("/*");
		return registrationBean;
	}
	
}
