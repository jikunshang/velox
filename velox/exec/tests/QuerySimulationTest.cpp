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
#include "velox/common/base/test_utils/GTestUtils.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/dwio/dwrf/test/utils/DataFiles.h"
#include "velox/exec/tests/utils/Cursor.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/type/Type.h"
#include "velox/type/tests/FilterBuilder.h"
#include "velox/type/tests/SubfieldFiltersBuilder.h"

#include <boost/filesystem.hpp>
using namespace boost::filesystem;

#if __has_include("filesystem")
#include <filesystem>
namespace fs = std::filesystem;
#else
#include <experimental/filesystem>
namespace fs = std::experimental::filesystem;
#endif

using namespace facebook::velox;
using namespace facebook::velox::connector::hive;
using namespace facebook::velox::exec;
using namespace facebook::velox::common::test;
using namespace facebook::velox::exec::test;

static const std::string kNodeSelectionStrategy = "node_selection_strategy";
static const std::string kSoftAffinity = "SOFT_AFFINITY";
static const std::string kTableScanTest = "TableScanTest.Writer";

class QuerySimulationTest : public virtual HiveConnectorTestBase,
                            public testing::WithParamInterface<bool> {
 protected:
  void SetUp() override {
    useAsyncCache_ = GetParam();
    HiveConnectorTestBase::SetUp();
    rowType_ =
        ROW({"l_quantity", "l_extendedprice", "l_discount", "l_shipdate"},
            {DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE()});
  }

  static void SetUpTestCase() {
    HiveConnectorTestBase::SetUpTestCase();
  }

  std::vector<RowVectorPtr> makeVectors(
      int32_t count,
      int32_t rowsPerVector,
      const std::shared_ptr<const RowType>& rowType = nullptr) {
    auto inputs = rowType ? rowType : rowType_;
    return HiveConnectorTestBase::makeVectors(inputs, count, rowsPerVector);
  }

  std::shared_ptr<Task> assertQuery(
      const std::shared_ptr<const core::PlanNode>& plan,
      const std::shared_ptr<HiveConnectorSplit>& hiveSplit,
      const std::string& duckDbSql) {
    return OperatorTestBase::assertQuery(plan, {hiveSplit}, duckDbSql);
  }

  std::shared_ptr<Task> assertQuery(
      const std::shared_ptr<const core::PlanNode>& plan,
      const exec::Split&& split,
      const std::string& duckDbSql) {
    return OperatorTestBase::assertQuery(plan, {split}, duckDbSql);
  }

  std::shared_ptr<Task> assertQuery(
      const std::shared_ptr<const core::PlanNode>& plan,
      const std::vector<std::shared_ptr<TempFilePath>>& filePaths,
      const std::string& duckDbSql) {
    return HiveConnectorTestBase::assertQuery(plan, filePaths, duckDbSql);
  }

  std::shared_ptr<facebook::velox::core::PlanNode> tableScanNode() {
    return PlanBuilder().tableScan(rowType_).planNode();
  }

  static std::shared_ptr<facebook::velox::core::PlanNode> tableScanNode(
      const std::shared_ptr<const RowType>& outputType) {
    return PlanBuilder().tableScan(outputType).planNode();
  }

  static OperatorStats getTableScanStats(const std::shared_ptr<Task>& task) {
    return task->taskStats().pipelineStats[0].operatorStats[0];
  }

  static int64_t getSkippedStridesStat(const std::shared_ptr<Task>& task) {
    return getTableScanStats(task).runtimeStats["skippedStrides"].sum;
  }

  static int64_t getSkippedSplitsStat(const std::shared_ptr<Task>& task) {
    return getTableScanStats(task).runtimeStats["skippedSplits"].sum;
  }

  void testNonPartitionedTableImpl(const std::vector<std::string>& file_list) {
    std::vector<std::shared_ptr<connector::ConnectorSplit>> connectorSplits;
    for (auto single_file_path : file_list) {
      auto split = std::make_shared<HiveConnectorSplit>(
        kHiveConnectorId,
        single_file_path,
        facebook::velox::dwio::common::FileFormat::ORC,
        0,
        fs::file_size(single_file_path));
      connectorSplits.push_back(split);
    }
    
    std::vector<exec::Split> splits;
    splits.reserve(connectorSplits.size());
    for (const auto& connectorSplit : connectorSplits) {
      splits.emplace_back(exec::Split(folly::copy(connectorSplit), -1));
    }
    std::cout << "splits size: " << splits.size() << std::endl;

    std::string c_0 = "l_quantity";
    std::string c_1 = "l_extendedprice";
    std::string c_2 = "l_discount";
    std::string c_3 = "l_shipdate_new";

    auto outputType =
        ROW({c_1, c_2}, {DOUBLE(), DOUBLE()});

    SubfieldFilters filters;
    filters[common::Subfield(c_3)] =
        std::make_unique<common::DoubleRange>(
            8766.0, false, false, 9131.0, false, true, false);
    filters[common::Subfield(c_0)] =
        std::make_unique<common::DoubleRange>(
            0, true, false, 24, false, true, false);
    filters[common::Subfield(c_2)] =
        std::make_unique<common::DoubleRange>(
            0.05, false, false, 0.07, false, false, false);
    auto tableHandle = makeTableHandle(std::move(filters), nullptr);

    std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>> assignments;
    assignments[c_1] = regularColumn(c_1, DOUBLE());
    assignments[c_2] = regularColumn(c_2, DOUBLE());

    auto op = PlanBuilder()
                  .tableScan(outputType, tableHandle, assignments)
                  // .filter("not(is_null(l_shipdate_new)) and not(is_null(l_discount)) and not(is_null(l_quantity)) and (l_shipdate_new >= 8766.0) and (l_shipdate_new < 9131.0) and (l_discount >= 0.05) and (l_discount <= 0.07) and (l_quantity < 24.00)")
                  .project(std::vector<std::string>{"l_extendedprice * l_discount"},
                           std::vector<std::string>{"mul_res"})
                  .aggregation(
                    {},
                    {"sum(mul_res)"},
                    {},
                    core::AggregationNode::Step::kPartial,
                    false)
                  .planNode();

    bool noMoreSplits = false;
    std::function<void(exec::Task*)> addSplits = [&](Task* task) {
      if (noMoreSplits) {
        return;
      }
      for (auto& split : splits) {
        task->addSplit("0", std::move(split));
      }
      task->noMoreSplits("0");
      noMoreSplits = true;
    };

    CursorParameters params;
    params.planNode = op;
    std::cout << "start read" << std::endl;
    auto result_pair = readCursor(params, addSplits);
    std::cout << "finish read" << std::endl;
    std::vector<RowVectorPtr> results = result_pair.second;
    std::cout << "result size: " << results.size() << std::endl;
    int64_t num_rows = 0;
    for (auto outputVector : results) {
      // std::cout << "out size: " << outputVector->size() << std::endl;
      num_rows += outputVector->size();
      // for (size_t i = 0; i < outputVector->size(); ++i) {
      //   std::cout << outputVector->toString(i) << std::endl;
      // }
    }
    std::cout << "num_rows: " << num_rows << std::endl;
  }

  void testNonPartitionedTable(const std::vector<std::string>& file_list) {
    testNonPartitionedTableImpl(file_list);
  }

  bool EndsWith(const std::string& data, const std::string& suffix) {
    return data.find(suffix, data.size() - suffix.size()) != std::string::npos;
  }

  std::shared_ptr<const RowType> rowType_{
      ROW({"l_quantity", "l_extendedprice", "l_discount", "l_shipdate_new"},
          {DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE()})};
};

TEST_P(QuerySimulationTest, nonPartitionedTable) {
  std::string dir = "/root/rui/parquets/lineitem_orcs/";
  path file_path(dir);
  std::vector<std::string> file_list;
  for (auto i = directory_iterator(file_path); i != directory_iterator(); i++) {
    if (!is_directory(i->path())) {
      std::string single_file_path = i->path().filename().string();
      if (EndsWith(single_file_path, ".orc")) {
        std::cout << single_file_path << std::endl;
        file_list.push_back(dir + single_file_path);
      }
    } else {
      continue;
    }
  }
  testNonPartitionedTable(file_list);
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    QuerySimulationTests,
    QuerySimulationTest,
    testing::Values(true, false));
