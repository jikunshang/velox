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
#pragma once

#include "velox/core/HybridPlanNode.h"
#include "velox/exec/Operator.h"

// FIXME: include order matters since omnisci header file is not clean yet and
// may define some dirty macro which will influence velox code base, so we put
// it at the end of include chain. This is just a work around, if some further
// code change have similar issue, best way is make header file cleaner.
#include "Cider/CiderExecutionKernel.h"
#include "QueryEngine/RelAlgExecutionUnit.h"
#include "QueryEngine/InputMetadata.h"

namespace facebook::velox::exec {
class HybridExecOperator : public Operator {
 public:
  HybridExecOperator(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const core::HybridPlanNode>& hybridPlanNode)
      : Operator(
            driverCtx,
            hybridPlanNode->outputType(),
            operatorId,
            hybridPlanNode->id(),
            "hybrid"),
            relAlgExecUnit_(hybridPlanNode->getCiderParamContext()->getExeUnitBasedOnContext()) {
    assert(relAlgExecUnit_);
    // TODO: we should know the query type

    // construct and compile a kernel here.
    ciderKernel_ = CiderExecutionKernel::create();
    // todo: we don't have input yet.
    ciderKernel_->compileWorkUnit(*relAlgExecUnit_, buildInputTableInfo());
    std::cout << "IR: " << ciderKernel_->getLlvmIR() << std::endl;
  };

  static PlanNodeTranslator planNodeTranslator;

  bool needsInput() const override;

  void addInput(RowVectorPtr input) override;

  BlockingReason isBlocked(ContinueFuture* future) override {
    return BlockingReason::kNotBlocked;
  };

  RowVectorPtr getOutput() override;

 private:
  int64_t totalRowsProcessed_ = 0;
  std::shared_ptr<CiderExecutionKernel> ciderKernel_;
  RowVectorPtr result_;

  bool isFilter = false;
  bool isAgg = false;
  bool isGroupBy = false;
  bool isSort = false;
  bool finished_ = false;
  bool isJoin = false;

  const std::shared_ptr<RelAlgExecutionUnit> relAlgExecUnit_;

  // init this according to input expressions.
  std::vector<int64_t> partialAggResult;
  RowVectorPtr tmpOut;

  void process();

  std::vector<InputTableInfo> buildInputTableInfo() {
    std::vector<InputTableInfo> query_infos;
    Fragmenter_Namespace::FragmentInfo fi_0;
    fi_0.fragmentId = 0;
    fi_0.shadowNumTuples = 1024;
    fi_0.physicalTableId = relAlgExecUnit_->input_descs[0].getTableId();
    fi_0.setPhysicalNumTuples(1024);

    Fragmenter_Namespace::TableInfo ti_0;
    ti_0.fragments = {fi_0};
    ti_0.setPhysicalNumTuples(1024);

    InputTableInfo iti_0{relAlgExecUnit_->input_descs[0].getTableId(), ti_0};
    query_infos.push_back(iti_0);

    return query_infos;
  }
};
} // namespace facebook::velox::exec
