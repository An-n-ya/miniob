/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by annya on 2023/12/16.
//

#include "sql/operator/update_physical_operator.h"
#include "sql/stmt/insert_stmt.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"

using namespace std;

UpdatePhysicalOperator::UpdatePhysicalOperator(Table *table, vector<Value> &&values, vector<string> &&attributes)
    : table_(table), values_(std::move(values)), attributes_(attributes)
{}

RC UpdatePhysicalOperator::open(Trx *trx)
{
  LOG_DEBUG("update physical operator [open] called, children size=%d", children_.size());
  if (children_.empty()) {
    return RC::SUCCESS;
  }
  LOG_DEBUG("update physical operator [open] children is not empty");

  std::unique_ptr<PhysicalOperator> &child = children_[0];
  RC rc = child->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  trx_ = trx;

  return RC::SUCCESS;
}

RC UpdatePhysicalOperator::make_record(RowTuple *tuple, Record &record)
{
  auto field_metas = *table_->table_meta().field_metas();
  auto table_name = table_->name();
  auto field_num = field_metas.size();
  auto values = std::vector<Value>(field_num);
  int idx = 0;
  RC rc;
  for (auto field_meta : field_metas) {
    auto field_name = field_meta.name();
    bool found_field = false;
    for (long unsigned int i = 0; i < attributes_.size(); i++) {
      if (attributes_[i].compare(field_name)) {
        values[idx++] = values_[i];
        found_field = true;
        break;
      }
    }
    if (found_field)
      continue ;
    auto spec = TupleCellSpec(table_name, field_name);
    Value value;
    rc = tuple->find_cell(spec, value);
    if (rc == RC::NOTFOUND) {
      LOG_WARN("cannot find corresponding cell given the field name=%s", field_name);
      return RC::SCHEMA_FIELD_TYPE_MISMATCH;
    }
    values[idx++] = value;
  }

  rc = table_->make_record(field_num, values.data(), record);
  return rc;
}

RC UpdatePhysicalOperator::next()
{
  LOG_DEBUG("update physical operator [next] called");
  if (children_.empty()) {
    return RC::RECORD_EOF;
  }
  LOG_DEBUG("update physical operator [next] children is not empty");

  RC rc = RC::SUCCESS;

  PhysicalOperator *child = children_[0].get();
  while (RC::SUCCESS == (rc = child->next())) {
    Tuple *tuple = child->current_tuple();
    if (nullptr == tuple) {
      LOG_WARN("failed to get current record: %s", strrc(rc));
      return rc;
    }

    RowTuple *row_tuple = static_cast<RowTuple *>(tuple);
    Record &target_record = row_tuple->record();

    Record record;
    make_record(row_tuple, record);
//    rc = table_->make_record(static_cast<int>(values_.size()), values_.data(), record);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to make record. rc=%s", strrc(rc));
      return rc;
    }

    rc = trx_->update_record(table_, target_record, record);
    LOG_DEBUG("update record returned!");
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to update record: %s", strrc(rc));
      return rc;
    }
  }
  return RC::RECORD_EOF;
}

RC UpdatePhysicalOperator::close()
{
  if (!children_.empty()) {
    children_[0]->close();
  }
  return RC::SUCCESS;
}
