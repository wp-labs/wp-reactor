//! 归并/求值/读取纯函数簇（键哈希、桶键求值、行字段与列式段辅助、domain 扫描）。
//! 子目录拆分：`rows`（字段/行字段/列读取）/ `merge`（分片归并）/ `keys`（键列
//! 预解析与哈希）/ `measure`（close 度量输出）/ `rowkey`（行式桶键求值）/
//! `domain`（行域扫描：count/sum/minmax/distinct）。对外经本文件统一
//! `pub(crate) use` 保持 `stats_exec::eval::*` 原路径不变。

use std::collections::HashMap;

use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use wf_lang::ast::{Expr, FieldRef};
use wf_lang::plan::{StatsAggPlan, StatsMeasurePlan, StatsPlan};

use super::*;
use super::row_acc::{max_fold, min_fold};
use crate::match_engine::Value;
use crate::match_engine::cep::ScopeKey;
use crate::match_engine::event_bridge::extract_field_value;
use crate::window::scope_key_from_column;
use wf_cep::rows::{RowFieldLayout, RowFields};

mod domain;
mod keys;
mod measure;
mod merge;
mod rowkey;
mod rows;

pub(crate) use self::{domain::*, keys::*, measure::*, merge::*, rowkey::*, rows::*};
