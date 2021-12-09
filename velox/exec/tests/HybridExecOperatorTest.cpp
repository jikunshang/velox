/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <folly/init/Init.h>
#include "velox/dwio/dwrf/test/utils/BatchMaker.h"
#include "velox/exec/Driver.h"
#include "velox/exec/tests/OperatorTestBase.h"
#include "velox/exec/tests/PlanBuilder.h"

// FIXME: Workaround dependency issue from omnisci and velox integrations
#include "velox/cider/VeloxPlanToCiderExecutionUnit.h"
#include "velox/core/HybridPlanNode.h"
#include "velox/exec/HybridExecOperator.h"

using namespace facebook::velox;
using namespace facebook::velox::core;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

using facebook::velox::test::BatchMaker;

class HybridExecOperatorTest : public OperatorTestBase {
 protected:
  void assertQueryPlan(
      const std::shared_ptr<core::PlanNode>& planNode,
      const std::string& duckDBSql) {
    facebook::velox::cider::CiderExecutionUnitGenerator generator;
    auto hybridPlan = generator.transformPlan(planNode);
    // TODO: we should verify whether this hybridPlan is valid.
    assertQuery(hybridPlan, duckDBSql);
  }

  std::shared_ptr<const RowType> rowType_{
      ROW({"c0", "c1", "c2", "c3"},
          {INTEGER(), DOUBLE(), INTEGER(), INTEGER()})};
};

// FIXME: this test will fail currently, since we didn't impl hybridOperator
TEST_F(HybridExecOperatorTest, complexNode) {
  Operator::registerOperator(HybridExecOperator::planNodeTranslator);
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 10; ++i) {
    auto vector = std::dynamic_pointer_cast<RowVector>(
        BatchMaker::createBatch(rowType_, 100, *pool_));
    vectors.push_back(vector);
  }
  createDuckDbTable(vectors);

  auto plan =
      PlanBuilder()
          .values(vectors)
          .filter("(c2 < 1000)")
          .project(
              std::vector<std::string>{"c0 * c1"},
              std::vector<std::string>{"e1"})
          .aggregation(
              {}, {"sum(e1)"}, {}, core::AggregationNode::Step::kPartial, false)
          .partitionedOutput({}, 1)
          .planNode();

  assertQueryPlan(plan, "SELECT SUM(c0 * c1) from tmp where c2 < 1000");
}
