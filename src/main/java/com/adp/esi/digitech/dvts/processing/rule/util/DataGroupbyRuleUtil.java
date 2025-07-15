package com.adp.esi.digitech.dvts.processing.rule.util;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;

import org.apache.commons.lang3.math.NumberUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.adp.esi.digitech.dvts.processing.autowire.service.CustomDataRuleUtilDynamicAutowireService;
import com.adp.esi.digitech.dvts.processing.ds.config.model.DataAggregation;
import com.adp.esi.digitech.dvts.processing.ds.config.model.DataFilter;
import com.adp.esi.digitech.dvts.processing.ds.config.model.DataGroupBy;
import com.adp.esi.digitech.dvts.processing.ds.config.model.DynamicClause;
import com.adp.esi.digitech.dvts.processing.exception.TransformationException;
import com.adp.esi.digitech.dvts.processing.model.Row;

import lombok.extern.slf4j.Slf4j;

@Service("dataGroupbyRuleUtil")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class DataGroupbyRuleUtil  extends AbstractDataRuleUtil<Collector<Row,?,List<Row>>, DataGroupBy> {
	
	@Autowired
	CustomDataRuleUtilDynamicAutowireService customDataRuleUtilDynamicAutowireService;

	public Collector<Row,?,List<Row>> construct(DataGroupBy dataGroupBy)  throws TransformationException {
		try {
			
			Function<Double, String> doubleToStringFunction = value -> {
				if(Objects.nonNull(value)) {
					var result = Double.toString(value);
					return result.contains(".") ? result.replaceAll("0*$", "").replaceAll("\\.$", "") : result;					
				}
				return "0";
			};
			
			log.info("DataGroupbyRuleUtil - construct(), Started costructing Group By clause, uniqueId = {}, datasetName = {}", requestContext.getRequestUuid(), datasetName);
			
			if(dataGroupBy.getColumns() == null || dataGroupBy.getColumns().length <= 0) {
				var message = "Columns configuration is mandatory for Group BY";
				log.error("DataGroupbyRuleUtil - construct()  Failed costructing Group By clause, UniqueId {}, datasetName = {}, message = {}",	requestContext.getUniqueId(), datasetName, message);
				var transformationException = new TransformationException("Transformation Error - " + message);
				transformationException.setRequestContext(requestContext);
				throw transformationException;
			}
			/*
			if(dataGroupBy.getAggregations() == null || dataGroupBy.getAggregations().size() <= 0) {
				var message = "Aggregations configuration is mandatory for Group BY";
				log.error("DataGroupbyRuleUtil - construct()  Failed costructing Group By clause, UniqueId {}, datasetName = {}, message = {}",	requestContext.getUniqueId(), datasetName, message);
				var transformationException = new TransformationException("Transformation Error - " + message);
				transformationException.setRequestContext(requestContext);
				throw transformationException;
			}
			*/
			Function<Row, String> classifier = row -> {			
				var columns = dataGroupBy.getColumns();
				return Stream.of(columns).map(column -> row.getColumns().get(UUID.fromString(column)).getSourceValue()).collect(Collectors.joining("_"));				
			};
			
			log.info("DataGroupbyRuleUtil - construct(), Completed costructing classifer, uniqueId = {}, datasetName = {}", 	requestContext.getRequestUuid(), datasetName);
			
			var groupCollector = Collectors.groupingBy(classifier);
			var aggregations = dataGroupBy.getAggregations();
			var filters = dataGroupBy.getFilters();
			var dynamicClauses = dataGroupBy.getClause();
			
			List<DynamicClause> groupDynamicClauses = Objects.nonNull(dynamicClauses) && !dynamicClauses.isEmpty() ?
					dynamicClauses.stream().filter(dynamicClause -> Objects.nonNull(dynamicClause.getLevel()) && dynamicClause.getLevel().equalsIgnoreCase("Group")).collect(Collectors.toList()) : null;
			
			log.info("groupDynamicClauses = {}", groupDynamicClauses);
			
			
			//rows.parallelStream().collect(Collectors.groupingBy(null), Collectors.filtering(null, null));
			
			BiFunction<Stream<Row>, Predicate<Row>, Stream<Row>> filterBiFunction = (rows,predicate) -> rows.filter(predicate);
			final Function<List<Row>, List<Row>> streamFilterFunction = (Objects.nonNull(filters) && !filters.isEmpty()) ?
					(rows) -> {						
									
						for (DataFilter filter : filters) {
							var stream = rows.parallelStream();	
							if(Objects.nonNull(filter) && Objects.nonNull(filter.getConditions()) && !filter.getConditions().isEmpty()) {
								var selectType = filter.getSelect();
								if(Objects.nonNull(groupDynamicClauses) && !groupDynamicClauses.isEmpty()) {
									var groupClauseMap = constructGroupClause(groupDynamicClauses, rows);									
									if(Objects.nonNull(groupClauseMap) && !groupClauseMap.isEmpty())										
										dynamicValues.putAll(groupClauseMap);									
								}
								var rowPredicate = customDataRuleUtilDynamicAutowireService.constructFilter(DataGroupbyFilterRuleUtil.class, filter, dynamicValues, datasetName, requestContext);
								if(selectType.equalsIgnoreCase("All")) {
									if(rows.parallelStream().anyMatch(rowPredicate))
										stream = filterBiFunction.apply(stream, row -> true);
									else
										stream = filterBiFunction.apply(stream, row -> false);
								}  else if(selectType.equalsIgnoreCase("Match")) {						
									stream = filterBiFunction.apply(stream, rowPredicate);	
								}
							}
							rows = stream.collect(Collectors.toList());
						}
						return rows;
					} : null;
			
			
			Function<Map<String, List<Row>>, List<Row>> finisher = map -> {				
				return map.entrySet().stream().map(entry -> {
					var rows = Objects.nonNull(streamFilterFunction) ? streamFilterFunction.apply(entry.getValue()) : entry.getValue();
					
					if (Objects.isNull(rows) || rows.isEmpty())	return null;
					
					Supplier<Row> rowSupplier = () -> {
						if(Objects.nonNull(groupDynamicClauses) && !groupDynamicClauses.isEmpty()) {
							var groupClauseMap = constructGroupClause(groupDynamicClauses, rows);
							if(Objects.nonNull(groupClauseMap) && !groupClauseMap.isEmpty())										
								dynamicValues.putAll(groupClauseMap);									
						}
						var filter = filters.get(filters.size()-1);
						var rowPredicate = customDataRuleUtilDynamicAutowireService.constructFilter(DataGroupbyFilterRuleUtil.class, filter, dynamicValues, datasetName, requestContext);
						return rows.parallelStream().filter(rowPredicate).findFirst().orElse(rows.get(0));							
					};
					
					Row temp = Objects.nonNull(filters) && !filters.isEmpty() ? rowSupplier.get() : rows.get(0);
					
					if(Objects.nonNull(aggregations) && !aggregations.isEmpty()) {						
						aggregations.forEach(aggregation -> {
							var result = summarize(rows, aggregation);
							temp.getColumns().get(UUID.fromString(aggregation.getColumn())).setValue(result);
							temp.getColumns().get(UUID.fromString(aggregation.getColumn())).setTransformedValue(result);
							temp.getColumns().get(UUID.fromString(aggregation.getColumn())).setTargetValue(doubleToStringFunction.apply(result));
						});
					}
					return temp;
				}).filter(Objects::nonNull).collect(Collectors.toList());
			};
			
			/*
			var filter = filters.get(0);
			if(filter != null && filter.getConditions() != null && !filter.getConditions().isEmpty()) {
				var selectType = filter.getSelect();
				Function<Map<String, List<Row>>, List<Row>> finisher = map -> {
					var rowPredicate = customDataRuleUtilDynamicAutowireService.constructFilter(DataGroupbyFilterRuleUtil.class,filter, dynamicValues, datasetName ,requestContext);
					return map.entrySet().parallelStream().filter(entry -> entry.getValue().parallelStream().anyMatch(rowPredicate)).map(entry -> {
						Row temp = entry.getValue().parallelStream().filter(rowPredicate).findFirst().orElse(entry.getValue().get(0));					
						if(selectType.equalsIgnoreCase("All")) {						
							aggregations.forEach(aggregation -> {
								var result = summarize(entry.getValue(), aggregation);
								temp.getColumns().get(UUID.fromString(aggregation.getColumn())).setValue(result);
								temp.getColumns().get(UUID.fromString(aggregation.getColumn())).setTargetValue(doubleToStringFunction.apply(result));
							});
						} else if(selectType.equalsIgnoreCase("Match")) {
							var matchedResults = entry.getValue().parallelStream().filter(rowPredicate).collect(Collectors.toList());
							
							aggregations.forEach(aggregation -> {
								var result = summarize(matchedResults, aggregation);
								temp.getColumns().get(UUID.fromString(aggregation.getColumn())).setValue(result);
								temp.getColumns().get(UUID.fromString(aggregation.getColumn())).setTargetValue(doubleToStringFunction.apply(result));
							});
						}
						return temp;
					}).collect(Collectors.toList());
					
				};
				var collector = Collectors.collectingAndThen(groupCollector, finisher);
				log.info("DataGroupbyFilterRuleUtil - construct(), Completed costructing Group By clause with filters, uniqueId = {}, datasetName = {}", requestContext.getRequestUuid(), datasetName);
				return collector;
			}
						
			Function<Map<String, List<Row>>, List<Row>> finisher = map ->  map.entrySet().parallelStream().map(entry -> {
					Row temp = entry.getValue().get(0);
					aggregations.forEach(aggregation -> {
						var result = summarize(entry.getValue(), aggregation);
						temp.getColumns().get(UUID.fromString(aggregation.getColumn())).setValue(result);
						temp.getColumns().get(UUID.fromString(aggregation.getColumn())).setTargetValue(doubleToStringFunction.apply(result));
					});
					return temp;
				}).collect(Collectors.toList());
			
			*/
			var collector = Collectors.collectingAndThen(groupCollector, finisher);
			log.info("DataGroupbyFilterRuleUtil - construct(), Completed costructing Group By clause with out filters, uniqueId = {}, datasetName = {}",	requestContext.getRequestUuid(), datasetName);
			return collector;
		} catch (Exception e) {
			log.error("DataGroupbyRuleUtil - construct()  Failed costructing Group By clause, UniqueId {}, datasetName = {}, message = {}", requestContext.getUniqueId(), datasetName, e.getMessage());
			var transformationException = new TransformationException("Transformation Error - " + e.getMessage(),e.getCause());
			transformationException.setRequestContext(requestContext);
			throw transformationException;
		}
		
	}
	
	private Double summarize(List<Row> rows, DataAggregation aggregation) {
		var column = aggregation.getColumn();
		BiFunction<Row, String, Object> sourceDataFunction = (row,uuid) -> row.getColumns().get(UUID.fromString(uuid)).getValue();
		Function<List<Row>, DoubleStream> rowsFilterFunction = (rowsData) -> rowsData.stream().map(row -> sourceDataFunction.apply(row, column)).filter(data -> Objects.nonNull(data) && NumberUtils.isParsable(data.toString())).mapToDouble(data -> Double.valueOf(data.toString()));
		
		switch (aggregation.getAggregator()) {
			case "sum":					
				return rowsFilterFunction.apply(rows).sum();				
			case "avg":
				return rowsFilterFunction.apply(rows).average().getAsDouble();				
			case "max":
				return rowsFilterFunction.apply(rows).max().getAsDouble();
			case "min":			
				return rowsFilterFunction.apply(rows).min().getAsDouble();
			default:
				break;
		}
		return null;
	}
	
	private Map<String, String> constructGroupClause(List<DynamicClause> dynamicClauses, List<Row> rows){
		
		Function<Double, String> doubleToStringFunction = value -> {
			if(Objects.nonNull(value)) {
				var result = Double.toString(value);
				return result.contains(".") ? result.replaceAll("0*$", "").replaceAll("\\.$", "") : result;					
			}
			return null;
		};
		
		
		BiFunction<Row, String, Object> sourceDataFunction = (row,uuid) -> row.getColumns().get(UUID.fromString(uuid)).getValue();
		BiFunction<List<Row>, String ,DoubleStream> rowsFilterFunction = (rowsData, column) -> rowsData.stream().map(row -> sourceDataFunction.apply(row, column)).filter(data -> Objects.nonNull(data)).mapToDouble(data -> Double.valueOf(data.toString()));
		//.filter(dynamicClause -> dynamicClause.getLevel().equalsIgnoreCase("Group"))
		return dynamicClauses.stream().collect(Collectors.toMap(DynamicClause::getName, clause -> {
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
