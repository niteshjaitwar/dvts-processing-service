package com.adp.esi.digitech.dvts.processing.filter.service;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.adp.esi.digitech.dvts.processing.autowire.service.CustomDataRuleUtilDynamicAutowireService;
import com.adp.esi.digitech.dvts.processing.ds.config.model.DataSetRules;
import com.adp.esi.digitech.dvts.processing.ds.config.model.DynamicClause;
import com.adp.esi.digitech.dvts.processing.enums.ProcessType;
import com.adp.esi.digitech.dvts.processing.exception.TransformationException;
import com.adp.esi.digitech.dvts.processing.model.Row;
import com.adp.esi.digitech.dvts.processing.rule.util.DataFilterRuleUtil;

import lombok.extern.slf4j.Slf4j;

@Service("dataFilterService")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class DataFilterService extends AbstractFilterService {

	@Value("#{${dynamic.values}}")
	private Map<String,String> dynamicValues;
	
	@Autowired
	CustomDataRuleUtilDynamicAutowireService customDataRuleUtilDynamicAutowireService;
	
	@Override
	public List<Row> filter(List<Row> data, DataSetRules dataSetRule) throws TransformationException {
		log.info("DataFilterService -> filter() Started applying data rules, uniqueId = {}, dataSet Name = {}", requestContext.getUniqueId(),dataSetRule.getDataSetName());
		
		if(dataSetRule.getFilters() != null && Objects.nonNull(dataSetRule.getFilters().getRules()) && !dataSetRule.getFilters().getRules().isEmpty()) {
			var rowPredicate = customDataRuleUtilDynamicAutowireService.constructFilter(DataFilterRuleUtil.class,dataSetRule.getFilters(), dynamicValues, dataSetRule.getDataSetName(), requestContext);
			data = data.parallelStream().filter(rowPredicate).collect(Collectors.toList());
			
			if(Objects.isNull(data) || data.isEmpty()) {
				log.error("DataFilterService -> filter() Processing Error - No Data found to process, After applying filters, uniqueId = {}, dataSet Name = {}", requestContext.getUniqueId(),dataSetRule.getDataSetName());
				var transformationException = new TransformationException("Processing Error - Dataset = '" + dataSetRule.getDataSetName() +"', No Data found to process, After applying filters");
				transformationException.setRequestContext(requestContext);
				throw transformationException;
			}
		}
		
		if(Objects.nonNull(dataSetRule.getGroupBy())) {
			if(processType.equals(ProcessType.in_memory) 
					&& Objects.nonNull(dataSetRule.getGroupBy()) 
					&& Objects.nonNull(dataSetRule.getGroupBy().getClause())) {
				dynamicValues.putAll(constructClause(dataSetRule.getGroupBy().getClause(), data));
			} else if(processType.equals(ProcessType.chunks)) {
				if(Objects.nonNull(dynamicClauseValues)) {
					log.info("dynamicClauseValues = {}", dynamicClauseValues);
					dynamicValues.putAll(dynamicClauseValues);
				}
			}		
			
			var groupByCollector = customDataRuleUtilDynamicAutowireService.constructGroupBy(dataSetRule.getGroupBy(), dynamicValues, dataSetRule.getDataSetName(), requestContext);
			return data.parallelStream().collect(groupByCollector);			
		}
		log.info("DataFilterService -> filter() Completed,  applying data rules, uniqueId = {}, dataSet Name = {}", requestContext.getUniqueId(),dataSetRule.getDataSetName());
		return data;		
	}
	
	private Map<String, String> constructClause(List<DynamicClause> dynamicClauses, List<Row> rows){
		
		Function<Double, String> doubleToStringFunction = value -> {
			if(Objects.nonNull(value)) {
				var result = Double.toString(value);
				return result.contains(".") ? result.replaceAll("0*$", "").replaceAll("\\.$", "") : result;					
			}
			return null;
		};
		
		
		BiFunction<Row, String, Object> sourceDataFunction = (row,uuid) -> row.getColumns().get(UUID.fromString(uuid)).getValue();
		BiFunction<List<Row>, String ,DoubleStream> rowsFilterFunction = (rowsData, column) -> rowsData.stream().map(row -> sourceDataFunction.apply(row, column)).filter(data -> Objects.nonNull(data)).mapToDouble(data -> Double.valueOf(data.toString()));
		
		return dynamicClauses.stream().filter(dynamicClause -> dynamicClause.getLevel().equalsIgnoreCase("Global")).collect(Collectors.toMap(DynamicClause::getName, clause -> {
			var column = clause.getColumn();
			if(clause.getOperator().equalsIgnoreCase("Max")) {
				return doubleToStringFunction.apply(rowsFilterFunction.apply(rows, column).max().getAsDouble());
			} else if(clause.getOperator().equalsIgnoreCase("Min")) {
				return doubleToStringFunction.apply(rowsFilterFunction.apply(rows, column).min().getAsDouble());
			}
			return null;
		}));
	}	
}
