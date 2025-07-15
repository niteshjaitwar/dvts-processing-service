package com.adp.esi.digitech.dvts.processing.rule.util;

import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.adp.esi.digitech.dvts.processing.ds.config.model.DataFilter;
import com.adp.esi.digitech.dvts.processing.exception.TransformationException;
import com.adp.esi.digitech.dvts.processing.model.Row;
import com.adp.esi.digitech.dvts.processing.util.ValidationUtil;

import lombok.extern.slf4j.Slf4j;

@Service("dataFilterRuleUtil")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class DataFilterRuleUtil extends AbstractDataRuleUtil<Predicate<Row>, DataFilter> {

	@Override
	public Predicate<Row> construct(DataFilter dataFilter) throws TransformationException {
		try {
			log.info("DataFilterRuleUtil - construct(), Started costructing predicate, uniqueId = {}, datasetName = {}",
					requestContext.getRequestUuid(), datasetName);

			var filtersList = dataFilter.getRules().stream().map(rule -> {
				
				var columnPredicatesList = rule.getConditions().stream().map(condition -> {
					return constructPredicate(condition);
				}).collect(Collectors.toList());

				var operator = rule.getOperator();
				
				if (columnPredicatesList.size() > 1 && !ValidationUtil.isHavingValue(operator)) {
					var transformationException = new TransformationException("Configuration Error - condition operator is required");
					transformationException.setRequestContext(requestContext);
					throw transformationException;
				}
				
				if(columnPredicatesList.size() == 1)
					return columnPredicatesList.stream().reduce(x -> true, Predicate::and);

				return this.constructPredicate(operator, columnPredicatesList);
				
			}).filter(Objects::nonNull).collect(Collectors.toList());

			var operator = dataFilter.getOperator();
			
			if (filtersList.size() > 1 && !ValidationUtil.isHavingValue(operator)) {
				var transformationException = new TransformationException("Configuration Error - rule operator is required");
				transformationException.setRequestContext(requestContext);
				throw transformationException;
			}
			
			if(filtersList.size() == 1)
				return filtersList.stream().reduce(x -> true, Predicate::and);
			
			log.info(
					"DataFilterRuleUtil - construct(), filter operator, uniqueId = {}, datasetName = {}, operator = {}",
					requestContext.getRequestUuid(), datasetName, operator);

			var predicate = this.constructPredicate(operator, filtersList);
					
			log.info(
					"DataFilterRuleUtil - construct(), Completed costructing predicate, uniqueId = {}, datasetName = {}",
					requestContext.getRequestUuid(), datasetName);
			return predicate;
		} catch (Exception e) {
			log.error(
					"DataFilterRuleUtil - construct()  Failed costructing predicate, UniqueId {}, name = {}, message = {}",
					requestContext.getUniqueId(), datasetName, e.getMessage());
			var transformationException = new TransformationException("Transformation Error - " + e.getMessage(),e.getCause());
			transformationException.setRequestContext(requestContext);
			throw transformationException;
		}
	}
}
