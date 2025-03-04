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

#include "velox/common/caching/DataCache.h"
#include "velox/connectors/hive/FileHandle.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/dwio/dwrf/common/CachedBufferedInput.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/dwio/dwrf/reader/ScanSpec.h"
#include "velox/dwio/dwrf/writer/Writer.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/expression/Expr.h"
#include "velox/type/Filter.h"
#include "velox/type/Subfield.h"

namespace facebook::velox::connector::hive {

class HiveColumnHandle : public ColumnHandle {
 public:
  enum class ColumnType { kPartitionKey, kRegular, kSynthesized };

  HiveColumnHandle(const std::string& name, ColumnType columnType)
      : name_(name), columnType_(columnType) {}

  const std::string& name() const {
    return name_;
  }

  ColumnType columnType() const {
    return columnType_;
  }

 private:
  const std::string name_;
  const ColumnType columnType_;
};

using SubfieldFilters =
    std::unordered_map<common::Subfield, std::unique_ptr<common::Filter>>;

class HiveTableHandle : public ConnectorTableHandle {
 public:
  HiveTableHandle(
      bool filterPushdownEnabled,
      SubfieldFilters subfieldFilters,
      const std::shared_ptr<const core::ITypedExpr>& remainingFilter)
      : filterPushdownEnabled_(filterPushdownEnabled),
        subfieldFilters_(std::move(subfieldFilters)),
        remainingFilter_(remainingFilter) {}

  bool isFilterPushdownEnabled() const {
    return filterPushdownEnabled_;
  }

  const SubfieldFilters& subfieldFilters() const {
    return subfieldFilters_;
  }

  const std::shared_ptr<const core::ITypedExpr>& remainingFilter() const {
    return remainingFilter_;
  }

 private:
  const bool filterPushdownEnabled_;
  const SubfieldFilters subfieldFilters_;
  const std::shared_ptr<const core::ITypedExpr> remainingFilter_;
};

/**
 * Represents a request for Hive write
 */
class HiveInsertTableHandle : public ConnectorInsertTableHandle {
 public:
  explicit HiveInsertTableHandle(const std::string& filePath)
      : filePath_(filePath) {}

  const std::string& filePath() const {
    return filePath_;
  }

  virtual ~HiveInsertTableHandle() {}

 private:
  const std::string filePath_;
};

class HiveDataSink : public DataSink {
 public:
  explicit HiveDataSink(
      std::shared_ptr<const RowType> inputType,
      const std::string& filePath,
      velox::memory::MemoryPool* FOLLY_NONNULL memoryPool);

  void appendData(VectorPtr input) override;

  void close() override;

 private:
  const std::shared_ptr<const RowType> inputType_;
  std::unique_ptr<facebook::velox::dwrf::Writer> writer_;
};

class HiveConnector;

class HiveDataSource : public DataSource {
 public:
  HiveDataSource(
      const std::shared_ptr<const RowType>& outputType,
      const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<connector::ColumnHandle>>& columnHandles,
      FileHandleFactory* FOLLY_NONNULL fileHandleFactory,
      velox::memory::MemoryPool* FOLLY_NONNULL pool,
      DataCache* FOLLY_NULLABLE dataCache,
      ExpressionEvaluator* FOLLY_NONNULL expressionEvaluator,
      memory::MappedMemory* FOLLY_NONNULL mappedMemory,
      const std::string& scanId,
      folly::Executor* FOLLY_NULLABLE executor);

  void addSplit(std::shared_ptr<ConnectorSplit> split) override;

  void addDynamicFilter(
      ChannelIndex outputChannel,
      const std::shared_ptr<common::Filter>& filter) override;

  RowVectorPtr next(uint64_t size) override;

  uint64_t getCompletedRows() override {
    return completedRows_;
  }

  uint64_t getCompletedBytes() override {
    return ioStats_->rawBytesRead();
  }

  std::unordered_map<std::string, int64_t> runtimeStats() override;

 private:
  // Evaluates remainingFilter_ on the specified vector. Returns number of rows
  // passed. Populates filterEvalCtx_.selectedIndices and selectedBits if only
  // some rows passed the filter. If no or all rows passed
  // filterEvalCtx_.selectedIndices and selectedBits are not updated.
  vector_size_t evaluateRemainingFilter(RowVectorPtr& rowVector);

  void setConstantValue(
      common::ScanSpec* FOLLY_NONNULL spec,
      const velox::variant& value) const;

  void setNullConstantValue(
      common::ScanSpec* FOLLY_NONNULL spec,
      const TypePtr& type) const;

  const std::shared_ptr<const RowType> outputType_;
  FileHandleFactory* FOLLY_NONNULL fileHandleFactory_;
  velox::memory::MemoryPool* FOLLY_NONNULL pool_;
  std::vector<std::string> regularColumns_;
  std::shared_ptr<dwio::common::IoStatistics> ioStats_;
  std::unique_ptr<dwrf::BufferedInputFactory> bufferedInputFactory_;
  std::unique_ptr<dwrf::ColumnReaderFactory> columnReaderFactory_;
  std::unique_ptr<common::ScanSpec> scanSpec_;
  std::shared_ptr<HiveConnectorSplit> split_;
  dwio::common::ReaderOptions readerOpts_;
  dwio::common::RowReaderOptions rowReaderOpts_;
  std::unique_ptr<dwrf::DwrfReader> reader_;
  std::unique_ptr<dwrf::DwrfRowReader> rowReader_;
  std::unique_ptr<exec::ExprSet> remainingFilterExprSet_;
  std::shared_ptr<const RowType> readerOutputType_;
  bool emptySplit_;

  // Number of splits skipped based on statistics.
  int64_t skippedSplits_{0};

  // Total bytes in splits skipped based on statistics.
  int64_t skippedSplitBytes_{0};

  // Number of strides (row groups) skipped based on statistics.
  int64_t skippedStrides_{0};

  VectorPtr output_;
  FileHandleCachedPtr fileHandle_;
  DataCache* FOLLY_NULLABLE dataCache_;
  ExpressionEvaluator* FOLLY_NONNULL expressionEvaluator_;
  uint64_t completedRows_ = 0;

  // Reusable memory for remaining filter evaluation
  VectorPtr filterResult_;
  SelectivityVector filterRows_;
  exec::FilterEvalCtx filterEvalCtx_;

  memory::MappedMemory* const FOLLY_NONNULL mappedMemory_;
  const std::string& scanId_;
  folly::Executor* FOLLY_NULLABLE executor_;
};

class HiveConnector final : public Connector {
 public:
  explicit HiveConnector(
      const std::string& id,
      std::shared_ptr<const Config> properties,
      std::unique_ptr<DataCache> dataCache,
      folly::Executor* FOLLY_NULLABLE executor);

  std::shared_ptr<DataSource> createDataSource(
      const std::shared_ptr<const RowType>& outputType,
      const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<connector::ColumnHandle>>& columnHandles,
      ConnectorQueryCtx* FOLLY_NONNULL connectorQueryCtx) override final {
    return std::make_shared<HiveDataSource>(
        outputType,
        tableHandle,
        columnHandles,
        &fileHandleFactory_,
        connectorQueryCtx->memoryPool(),
        connectorQueryCtx->config()->get<std::string>(
            kNodeSelectionStrategy, kNodeSelectionStrategyNoPreference) ==
                kNodeSelectionStrategySoftAffinity
            ? dataCache_.get()
            : nullptr,
        connectorQueryCtx->expressionEvaluator(),
        connectorQueryCtx->mappedMemory(),
        connectorQueryCtx->scanId(),
        executor_);
  }

  std::shared_ptr<DataSink> createDataSink(
      std::shared_ptr<const RowType> inputType,
      std::shared_ptr<ConnectorInsertTableHandle> connectorInsertTableHandle,
      ConnectorQueryCtx* FOLLY_NONNULL connectorQueryCtx) override final {
    auto hiveInsertHandle = std::dynamic_pointer_cast<HiveInsertTableHandle>(
        connectorInsertTableHandle);
    VELOX_CHECK(
        hiveInsertHandle != nullptr,
        "Hive connector expecting hive write handle!");
    return std::make_shared<HiveDataSink>(
        inputType,
        hiveInsertHandle->filePath(),
        connectorQueryCtx->memoryPool());
  }

  folly::Executor* FOLLY_NULLABLE executor() {
    return executor_;
  }

 private:
  std::unique_ptr<DataCache> dataCache_;
  FileHandleFactory fileHandleFactory_;
  folly::Executor* FOLLY_NULLABLE executor_;

  static constexpr const char* FOLLY_NONNULL kNodeSelectionStrategy =
      "node_selection_strategy";
  static constexpr const char* FOLLY_NONNULL
      kNodeSelectionStrategyNoPreference = "NO_PREFERENCE";
  static constexpr const char* FOLLY_NONNULL
      kNodeSelectionStrategySoftAffinity = "SOFT_AFFINITY";
};

class HiveConnectorFactory : public ConnectorFactory {
 public:
  HiveConnectorFactory() : ConnectorFactory(kHiveConnectorName) {
    dwio::common::FileSink::registerFactory();
  }

  std::shared_ptr<Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const Config> properties,
      std::unique_ptr<DataCache> dataCache = nullptr,
      folly::Executor* FOLLY_NULLABLE executor = nullptr) override {
    return std::make_shared<HiveConnector>(
        id, properties, std::move(dataCache), executor);
  }
};

} // namespace facebook::velox::connector::hive
