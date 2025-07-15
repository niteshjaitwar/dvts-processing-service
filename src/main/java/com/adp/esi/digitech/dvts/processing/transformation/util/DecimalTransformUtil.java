package com.adp.esi.digitech.dvts.processing.transformation.util;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.json.JSONObject;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.adp.esi.digitech.dvts.processing.exception.TransformationException;
import com.adp.esi.digitech.dvts.processing.model.Column;
import com.adp.esi.digitech.dvts.processing.model.Row;
import com.adp.esi.digitech.dvts.processing.util.ValidationUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service("decimalTransformUtil")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class DecimalTransformUtil extends AbstractTransformUtil {
	
	@Override
	public void transform(Row row, Column column, JSONObject rule) throws TransformationException {
		try {
			
			if(Objects.nonNull(rule) && rule.has("target_format") && !rule.isNull("target_format") && Objects.nonNull(column.getTransformedValue())) {				
				
				JSONObject targetFormatJson = rule.getJSONObject("target_format");		
				
				var targetType = targetFormatJson.has("targetType") && !targetFormatJson.isNull("targetType") ? targetFormatJson.getString("targetType") : null;
				var celldata = column.getTransformedValue().toString();
				celldata = celldata.contains(".") ? celldata.replaceAll("0*$", "").replaceAll("\\.$", "") : celldata;
				
				if(ValidationUtil.isHavingValue(targetType) && targetType.equalsIgnoreCase("Number")){
					
					BiFunction<String, Integer, DecimalFormat> formatFun = (format,targetDecimalAllowed) -> {
						var joined = targetDecimalAllowed.intValue() > 0 ? IntStream.range(0, targetDecimalAllowed).mapToObj(i -> "0").collect(Collectors.joining("")) : "";	
						DecimalFormat df = new DecimalFormat();
						DecimalFormatSymbols symbols = new DecimalFormatSymbols();			
						format = format.trim().toLowerCase();
						switch (format) {
						case "xx xxx,xx":								
							symbols.setDecimalSeparator(',');
							symbols.setGroupingSeparator(' ');
							
							break;
						case "xx.xxx,xx":								
							symbols.setDecimalSeparator(',');
							symbols.setGroupingSeparator('.');
							
							break;								
						case "xx,xxx.xx":								
							symbols.setDecimalSeparator('.');
							symbols.setGroupingSeparator(',');
							
							break;								
						case "xx'xxx,xx":								
							symbols.setDecimalSeparator(',');
							symbols.setGroupingSeparator('\'');
															
							break;
						case "xxxxx.xx":	
							symbols.setDecimalSeparator('.');
							df.applyPattern("######");						
							if (Objects.nonNull(joined)&& !joined.isBlank()) {
								df.applyPattern("######." + joined);
							}
							break;
						case "xxxxx,xx":	
							symbols.setDecimalSeparator(',');
							df.applyPattern("######");						
							if (Objects.nonNull(joined)&& !joined.isBlank()) {
								df.applyPattern("######." + joined);
							}
							break;	
						default:
							break;
						}
						
						df.setDecimalFormatSymbols(symbols);
						df.setRoundingMode(RoundingMode.HALF_UP);	
						
						if(!format.equalsIgnoreCase("xxxxx.xx") && !format.equalsIgnoreCase("xxxxx,xx")) {
							df.applyPattern("###,###");
							if (Objects.nonNull(joined) && !joined.isBlank())
								df.applyPattern("###,###." + joined);
						}
						
						return df;
										
					};
					
					var format = targetFormatJson.has("targetFormat") && !targetFormatJson.isNull("targetFormat") ? targetFormatJson.getString("targetFormat") :null;
					var targetDecimalAllowed = targetFormatJson.has("targetDecimalAllowed") && !targetFormatJson.isNull("targetDecimalAllowed") ? targetFormatJson.getInt("targetDecimalAllowed") : null;
					
					if(Objects.isNull(targetDecimalAllowed) && Objects.isNull(format)) {
						column.setTargetValue(celldata);
						return;
					} else {
						if(Objects.nonNull(targetDecimalAllowed) && Objects.nonNull(format)) {
							var range = Integer.valueOf(targetDecimalAllowed);
							var df = formatFun.apply(format, range);												
							var temp = df.format(Double.parseDouble(celldata));							
							column.setTargetValue(temp.toString());					
						} else if(Objects.nonNull(targetDecimalAllowed)) {					
							var temp = String.format("%."+targetDecimalAllowed + "f", Double.valueOf(celldata));
							column.setTargetValue(temp.toString());
						} else if(Objects.nonNull(format)) {
							var range = celldata.contains(".") ? celldata.split("\\.")[1].length() : 0;							
							var df = formatFun.apply(format, range);											
							var temp = df.format(Double.parseDouble(celldata));	
							column.setTargetValue(temp);	
						}
					}					
				}
			}
			
		} catch(Exception e) {
			log.error("DecimalTransformUtil - transform(), Failed Data Transformation requestUuid = {}, column = {}, message = {}", requestContext.getRequestUuid(), Objects.nonNull(column)? column.getName():"", e.getMessage());
			
			var transformationException = new TransformationException("Transformation Error - " + e.getMessage(), e.getCause());
			transformationException.setRequestContext(requestContext);
			throw transformationException;
		}
		
	}
}
