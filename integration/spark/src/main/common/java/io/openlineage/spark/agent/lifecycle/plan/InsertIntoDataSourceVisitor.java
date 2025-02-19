package io.openlineage.spark.agent.lifecycle.plan;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.PlanUtils;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.InsertIntoDataSourceCommand;

/**
 * {@link LogicalPlan} visitor that matches an {@link InsertIntoDataSourceCommand} and extracts the
 * output {@link OpenLineage.Dataset} being written.
 */
public class InsertIntoDataSourceVisitor
    extends QueryPlanVisitor<InsertIntoDataSourceCommand, OpenLineage.Dataset> {
  private final List<QueryPlanVisitor<? extends LogicalPlan, OpenLineage.Dataset>> datasetProviders;

  public InsertIntoDataSourceVisitor(
      List<QueryPlanVisitor<? extends LogicalPlan, OpenLineage.Dataset>> datasetProviders) {
    this.datasetProviders = datasetProviders;
  }

  @Override
  public List<OpenLineage.Dataset> apply(LogicalPlan x) {

    return PlanUtils.applyFirst(
            datasetProviders, ((InsertIntoDataSourceCommand) x).logicalRelation())
        .stream()
        // constructed datasets don't include the output stats, so add that facet here
        .peek(
            ds -> {
              Builder<String, OpenLineage.DatasetFacet> facetsMap =
                  ImmutableMap.<String, OpenLineage.DatasetFacet>builder();
              if (ds.getFacets().getAdditionalProperties() != null) {
                facetsMap.putAll(ds.getFacets().getAdditionalProperties());
              }
              ds.getFacets().getAdditionalProperties().putAll(facetsMap.build());
            })
        .collect(Collectors.toList());
  }
}
