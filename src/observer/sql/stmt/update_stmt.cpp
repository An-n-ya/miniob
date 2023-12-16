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
// Created by Wangyunlai on 2022/5/22.
//

#include "sql/stmt/update_stmt.h"

#include <utility>
#include "storage/db/db.h"
#include "common/log/log.h"
#include "storage/table/table.h"
#include "filter_stmt.h"

UpdateStmt::UpdateStmt(Table *table,
    FilterStmt *filter_stmt,
    const std::pair<std::string, Value> *values,
    int value_amount)
    : table_(table), values_(values), filter_stmt_(filter_stmt), value_amount_(value_amount)
{}

RC UpdateStmt::create(Db *db, const UpdateSqlNode &update, Stmt *&stmt)
{
  const char *table_name = update.relation_name.c_str();
  if (db == nullptr
      || table_name == nullptr
      || update.values.data() == nullptr) {
    LOG_WARN("invalid argument. dp=%p, table_name=%p",
          db, table_name);
    return RC::INVALID_ARGUMENT;
  }


  Table *table = db->find_table(table_name);
  if (table == nullptr) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  const TableMeta &table_meta = table->table_meta();

  for (auto pair : update.values) {
    const char *attr_name = pair.first.c_str();
    const FieldMeta *field_meta = table_meta.find_field_by_name(attr_name);
    if (nullptr == field_meta) {
      LOG_WARN("no such field. table_name=%s, field=%s", table_name, attr_name);
      return RC::SCHEMA_FIELD_NOT_EXIST;
    }

    const AttrType attr_type  = field_meta->type();
    const AttrType update_type = pair.second.attr_type();
    if (attr_type != update_type) {
      LOG_WARN("field type mismatch. table=%s, field=%s, field type=%d, value_type=%d",
               table_name, field_meta->name(), attr_type, update_type);
      return RC::SCHEMA_FIELD_TYPE_MISMATCH;
    }
  }

  const std::pair<std::string, Value> *values = update.values.data();
  const int value_num = static_cast<int>(update.values.size());

  std::unordered_map<std::string, Table *> table_map;
  table_map.insert(std::pair<std::string, Table *>(std::string(table_name), table));
  FilterStmt *filter_stmt = nullptr;
  RC rc = FilterStmt::create(
      db, table, &table_map, update.conditions.data(), static_cast<int>(update.conditions.size()), filter_stmt);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create filter statement. rc=%d:%s", rc, strrc(rc));
    return rc;
  }

  stmt = new UpdateStmt(table, filter_stmt, values, value_num);

  return RC::SUCCESS;
}
