use crate::schema::{BaseType, FieldType};

pub const WFU_PREFIX: &str = "__wfu_";

pub const WFU_ID: &str = "__wfu_id";
pub const WFU_RULE_NAME: &str = "__wfu_rule_name";
pub const WFU_SCORE: &str = "__wfu_score";
pub const WFU_ENTITY_TYPE: &str = "__wfu_entity_type";
pub const WFU_ENTITY_ID: &str = "__wfu_entity_id";
pub const WFU_ORIGIN: &str = "__wfu_origin";
pub const WFU_CLOSE_REASON: &str = "__wfu_close_reason";
pub const WFU_FIRED_AT: &str = "__wfu_fired_at";
pub const WFU_EMIT_TIME: &str = "__wfu_emit_time";
pub const WFU_SUMMARY: &str = "__wfu_summary";

#[derive(::moju_derive::MoJu, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[moju(kind = "state", domain = "Lang", module = "Lang.WfuMeta")]
pub enum WfuMetaField {
    Id,
    RuleName,
    Score,
    EntityType,
    EntityId,
    Origin,
    CloseReason,
    FiredAt,
    EmitTime,
    Summary,
}

pub const WFU_META_FIELDS: &[WfuMetaField] = &[
    WfuMetaField::Id,
    WfuMetaField::RuleName,
    WfuMetaField::Score,
    WfuMetaField::EntityType,
    WfuMetaField::EntityId,
    WfuMetaField::Origin,
    WfuMetaField::CloseReason,
    WfuMetaField::FiredAt,
    WfuMetaField::EmitTime,
    WfuMetaField::Summary,
];

#[derive(::moju_derive::MoJu, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[moju(kind = "state", domain = "Lang", module = "Lang.WfuIntermediateMeta")]
pub enum WfuIntermediateMetaField {
    RuleName,
    Score,
    EntityType,
    EntityId,
}

pub const WFU_INTERMEDIATE_META_FIELDS: &[WfuIntermediateMetaField] = &[
    WfuIntermediateMetaField::RuleName,
    WfuIntermediateMetaField::Score,
    WfuIntermediateMetaField::EntityType,
    WfuIntermediateMetaField::EntityId,
];

impl WfuMetaField {
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            WFU_ID => Some(Self::Id),
            WFU_RULE_NAME => Some(Self::RuleName),
            WFU_SCORE => Some(Self::Score),
            WFU_ENTITY_TYPE => Some(Self::EntityType),
            WFU_ENTITY_ID => Some(Self::EntityId),
            WFU_ORIGIN => Some(Self::Origin),
            WFU_CLOSE_REASON => Some(Self::CloseReason),
            WFU_FIRED_AT => Some(Self::FiredAt),
            WFU_EMIT_TIME => Some(Self::EmitTime),
            WFU_SUMMARY => Some(Self::Summary),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Id => WFU_ID,
            Self::RuleName => WFU_RULE_NAME,
            Self::Score => WFU_SCORE,
            Self::EntityType => WFU_ENTITY_TYPE,
            Self::EntityId => WFU_ENTITY_ID,
            Self::Origin => WFU_ORIGIN,
            Self::CloseReason => WFU_CLOSE_REASON,
            Self::FiredAt => WFU_FIRED_AT,
            Self::EmitTime => WFU_EMIT_TIME,
            Self::Summary => WFU_SUMMARY,
        }
    }

    pub fn base_type(self) -> BaseType {
        match self {
            Self::Score => BaseType::Float,
            Self::Id
            | Self::RuleName
            | Self::EntityType
            | Self::EntityId
            | Self::Origin
            | Self::CloseReason
            | Self::FiredAt
            | Self::EmitTime
            | Self::Summary => BaseType::Chars,
        }
    }

    pub fn field_type(self) -> FieldType {
        FieldType::Base(self.base_type())
    }

    pub fn available_in_yield(self) -> bool {
        true
    }
}

impl WfuIntermediateMetaField {
    pub fn meta_field(self) -> WfuMetaField {
        match self {
            Self::RuleName => WfuMetaField::RuleName,
            Self::Score => WfuMetaField::Score,
            Self::EntityType => WfuMetaField::EntityType,
            Self::EntityId => WfuMetaField::EntityId,
        }
    }

    pub fn name(self) -> &'static str {
        self.meta_field().name()
    }

    pub fn base_type(self) -> BaseType {
        self.meta_field().base_type()
    }

    pub fn field_type(self) -> FieldType {
        self.meta_field().field_type()
    }
}

pub fn is_wfu_meta_name(name: &str) -> bool {
    name.starts_with(WFU_PREFIX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intermediate_meta_directory_is_restricted_subset() {
        let fields: Vec<WfuMetaField> = WFU_INTERMEDIATE_META_FIELDS
            .iter()
            .map(|field| field.meta_field())
            .collect();

        assert_eq!(
            fields,
            vec![
                WfuMetaField::RuleName,
                WfuMetaField::Score,
                WfuMetaField::EntityType,
                WfuMetaField::EntityId,
            ]
        );
    }

    #[test]
    fn intermediate_meta_directory_exposes_names_and_types() {
        let fields: Vec<(&str, BaseType)> = WFU_INTERMEDIATE_META_FIELDS
            .iter()
            .map(|field| (field.name(), field.base_type()))
            .collect();

        assert_eq!(
            fields,
            vec![
                (WFU_RULE_NAME, BaseType::Chars),
                (WFU_SCORE, BaseType::Float),
                (WFU_ENTITY_TYPE, BaseType::Chars),
                (WFU_ENTITY_ID, BaseType::Chars),
            ]
        );
    }
}
