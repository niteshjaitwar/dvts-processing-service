package com.adp.esi.digitech.dvts.processing.request.filter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.IOUtils;
import org.springframework.http.HttpHeaders;
import org.springframework.mock.web.DelegatingServletInputStream;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;

public class GzipRequestFilter implements Filter {

	
	@Override
	public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
			throws IOException, ServletException {
		
		HttpServletRequest httpRequest = (HttpServletRequest) request;
		if("gzip".equalsIgnoreCase(httpRequest.getHeader(HttpHeaders.CONTENT_ENCODING))) {	
			GZIPInputStream gzipInputStream = new GZIPInputStream(httpRequest.getInputStream());
			var decompressedBody = IOUtils.toString(gzipInputStream, request.getCharacterEncoding());
			HttpServletRequest wrappedRequest = new HttpServletRequestWrapper(httpRequest) {
				@Override
				public ServletInputStream getInputStream() throws IOException {						
					return new DelegatingServletInputStream(new ByteArrayInputStream(decompressedBody.getBytes()));
				}
					
			};
			chain.doFilter(wrappedRequest, response);
		} else {		
			chain.doFilter(request, response);
		}
	}
	
}
