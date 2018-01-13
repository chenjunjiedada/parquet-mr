/* 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.filter2.recordlevel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.FilterPredicate.Visitor;
import org.apache.parquet.filter2.predicate.Operators.And;
import org.apache.parquet.filter2.predicate.Operators.Not;
import org.apache.parquet.filter2.predicate.Operators.Or;
import org.apache.parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicate.ValueInspector;
import org.apache.parquet.io.PrimitiveColumnIO;
import org.apache.parquet.schema.PrimitiveComparator;

import static org.apache.parquet.Preconditions.checkArgument;

/**
 * The implementation of this abstract class is auto-generated by
 * {@link org.apache.parquet.filter2.IncrementallyUpdatedFilterPredicateGenerator}
 *
 * Constructs a {@link IncrementallyUpdatedFilterPredicate} from a {@link org.apache.parquet.filter2.predicate.FilterPredicate}
 * This is how records are filtered during record assembly. The implementation is generated in order to avoid autoboxing.
 *
 * Note: the supplied predicate must not contain any instances of the not() operator as this is not
 * supported by this filter.
 *
 * the supplied predicate should first be run through {@link org.apache.parquet.filter2.predicate.LogicalInverseRewriter} to rewrite it
 * in a form that doesn't make use of the not() operator.
 *
 * the supplied predicate should also have already been run through
 * {@link org.apache.parquet.filter2.predicate.SchemaCompatibilityValidator}
 * to make sure it is compatible with the schema of this file.
 *
 * TODO: UserDefinedPredicates still autobox however
 */
public abstract class IncrementallyUpdatedFilterPredicateBuilderBase implements Visitor<IncrementallyUpdatedFilterPredicate> {
  private boolean built = false;
  private final Map<ColumnPath, List<ValueInspector>> valueInspectorsByColumn = new HashMap<ColumnPath, List<ValueInspector>>();
  private final Map<ColumnPath, PrimitiveComparator<?>> comparatorsByColumn = new HashMap<>();

  @Deprecated
  public IncrementallyUpdatedFilterPredicateBuilderBase() { }

  public IncrementallyUpdatedFilterPredicateBuilderBase(List<PrimitiveColumnIO> leaves) {
    for (PrimitiveColumnIO leaf : leaves) {
      ColumnDescriptor descriptor = leaf.getColumnDescriptor();
      ColumnPath path = ColumnPath.get(descriptor.getPath());
      PrimitiveComparator<?> comparator = descriptor.getPrimitiveType().comparator();
      comparatorsByColumn.put(path, comparator);
    }
  }

  public final IncrementallyUpdatedFilterPredicate build(FilterPredicate pred) {
    checkArgument(!built, "This builder has already been used");
    IncrementallyUpdatedFilterPredicate incremental = pred.accept(this);
    built = true;
    return incremental;
  }

  protected final void addValueInspector(ColumnPath columnPath, ValueInspector valueInspector) {
    List<ValueInspector> valueInspectors = valueInspectorsByColumn.get(columnPath);
    if (valueInspectors == null) {
      valueInspectors = new ArrayList<ValueInspector>();
      valueInspectorsByColumn.put(columnPath, valueInspectors);
    }
    valueInspectors.add(valueInspector);
  }

  public Map<ColumnPath, List<ValueInspector>> getValueInspectorsByColumn() {
    return valueInspectorsByColumn;
  }

  @SuppressWarnings("unchecked")
  protected final <T> PrimitiveComparator<T> getComparator(ColumnPath path) {
    return (PrimitiveComparator<T>) comparatorsByColumn.get(path);
  }

  @Override
  public final IncrementallyUpdatedFilterPredicate visit(And and) {
    return new IncrementallyUpdatedFilterPredicate.And(and.getLeft().accept(this), and.getRight().accept(this));
  }

  @Override
  public final IncrementallyUpdatedFilterPredicate visit(Or or) {
    return new IncrementallyUpdatedFilterPredicate.Or(or.getLeft().accept(this), or.getRight().accept(this));
  }

  @Override
  public final IncrementallyUpdatedFilterPredicate visit(Not not) {
    throw new IllegalArgumentException(
        "This predicate contains a not! Did you forget to run this predicate through LogicalInverseRewriter? " + not);
  }

}
