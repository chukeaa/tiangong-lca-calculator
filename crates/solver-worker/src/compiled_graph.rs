use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::graph_types::ScopeProcessPartition;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompiledFlowKind {
    Product,
    Elementary,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompiledFlowSpace {
    Technosphere,
    Biosphere,
    #[default]
    Reporting,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompiledSourceFlowType {
    Product,
    Waste,
    Elementary,
    #[default]
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum CompiledExchangeDirection {
    Input,
    Output,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompiledEdgePartition {
    PublicToPublic,
    PublicToPrivate,
    PrivateToPublic,
    PrivateToPrivate,
}

impl CompiledEdgePartition {
    #[must_use]
    pub fn from_partitions(
        provider: ScopeProcessPartition,
        consumer: ScopeProcessPartition,
    ) -> Self {
        match (provider, consumer) {
            (ScopeProcessPartition::Public, ScopeProcessPartition::Public) => Self::PublicToPublic,
            (ScopeProcessPartition::Public, ScopeProcessPartition::Private) => {
                Self::PublicToPrivate
            }
            (ScopeProcessPartition::Private, ScopeProcessPartition::Public) => {
                Self::PrivateToPublic
            }
            (ScopeProcessPartition::Private, ScopeProcessPartition::Private) => {
                Self::PrivateToPrivate
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompiledProcess {
    pub process_idx: i32,
    pub process_id: Uuid,
    pub process_version: String,
    pub process_name: Option<String>,
    pub model_id: Option<Uuid>,
    pub location: Option<String>,
    pub reference_year: Option<i32>,
    #[serde(default)]
    pub annual_supply_or_production_volume: Option<f64>,
    pub partition: ScopeProcessPartition,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompiledFlow {
    pub flow_idx: i32,
    pub flow_id: Uuid,
    #[serde(default)]
    pub flow_version: String,
    pub kind: CompiledFlowKind,
    #[serde(default)]
    pub space: CompiledFlowSpace,
    #[serde(default)]
    pub source_type: CompiledSourceFlowType,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompiledReferencePort {
    pub process_idx: i32,
    pub flow_id: Uuid,
    #[serde(default)]
    pub flow_version: String,
    pub reference_exchange_internal_id: String,
    pub coefficient: f64,
    pub raw_direction: CompiledExchangeDirection,
    pub raw_amount: f64,
    pub signed_raw_coefficient: f64,
    pub source_flow_type: CompiledSourceFlowType,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompiledBalanceResolution {
    pub dependent_process_idx: i32,
    pub residual_exchange_internal_id: String,
    pub balancing_process_idx: i32,
    pub balancing_reference_exchange_internal_id: String,
    pub flow_id: Uuid,
    #[serde(default)]
    pub flow_version: String,
    pub residual_coefficient: f64,
    pub reference_coefficient: f64,
    pub routing_weight: f64,
    pub activity_requirement: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompiledUnresolvedBalance {
    pub dependent_process_idx: i32,
    pub residual_exchange_internal_id: String,
    pub flow_id: Uuid,
    #[serde(default)]
    pub flow_version: String,
    pub residual_coefficient: f64,
    pub required_reference_sign: f64,
    pub candidate_count: i32,
    pub opposite_sign_candidate_count: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompiledProviderAllocation {
    pub provider_idx: i32,
    pub weight: f64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompiledProviderCandidateEligibility {
    #[default]
    Unknown,
    AcceptedReferenceOutput,
    RejectedNonReferenceOutput,
    AcceptedOppositeSignReference,
    RejectedSameSignReference,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompiledProviderOutputAllocationState {
    #[default]
    Unknown,
    Present,
    Missing,
    Invalid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompiledProviderCandidate {
    pub provider_idx: i32,
    pub provider_id: Uuid,
    #[serde(default)]
    pub output_exchange_internal_id: Option<String>,
    #[serde(default)]
    pub output_exchange_is_reference: bool,
    #[serde(default)]
    pub output_normalized_amount: Option<f64>,
    #[serde(default)]
    pub output_allocation_state: CompiledProviderOutputAllocationState,
    #[serde(default)]
    pub eligibility: CompiledProviderCandidateEligibility,
    #[serde(default)]
    pub process_name: Option<String>,
    #[serde(default)]
    pub location: Option<String>,
    #[serde(default)]
    pub reference_year: Option<i32>,
    #[serde(default)]
    pub annual_supply_or_production_volume: Option<f64>,
    #[serde(default)]
    pub reference_exchange_internal_id: Option<String>,
    #[serde(default)]
    pub reference_coefficient: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompiledProviderDecisionKind {
    UniqueProvider,
    MultiResolved,
    MultiUnresolved,
    NoProvider,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompiledProviderResolutionStrategy {
    UniqueProvider,
    BestProviderStrict,
    SplitByEvidence,
    SplitByProcessVolume,
    SplitEqual,
    SplitEqualFallback,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompiledProviderGeographyTier {
    LocalSubnational,
    SameCountry,
    SameRegion,
    Global,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompiledProviderSupplyRegionSource {
    ExchangeLocation,
    ConsumerProcessLocation,
    Unspecified,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompiledProviderFailureReason {
    NoProviderCandidates,
    RejectedNonReferenceOnly,
    RuleRequiresUniqueProvider,
    NoCandidateGeMinScore,
    Top1BelowTop1MinScore,
    Top1Top2RatioTooClose,
    ScoreSumNonPositive,
    NoOppositeSignReference,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompiledProviderDecision {
    pub consumer_idx: i32,
    pub flow_id: Uuid,
    #[serde(default)]
    pub flow_version: String,
    pub candidate_provider_count: i32,
    pub matched_provider_count: i32,
    #[serde(default)]
    pub candidates: Vec<CompiledProviderCandidate>,
    #[serde(default)]
    pub decision_kind: Option<CompiledProviderDecisionKind>,
    #[serde(default)]
    pub resolution_strategy: Option<CompiledProviderResolutionStrategy>,
    #[serde(default)]
    pub failure_reason: Option<CompiledProviderFailureReason>,
    pub used_equal_fallback: bool,
    #[serde(default)]
    pub volume_fallback_to_one_count: i32,
    #[serde(default)]
    pub geography_tier: Option<CompiledProviderGeographyTier>,
    #[serde(default)]
    pub supply_region_source: Option<CompiledProviderSupplyRegionSource>,
    #[serde(default)]
    pub supply_region_location: Option<String>,
    #[serde(default)]
    pub exchange_location_present: bool,
    pub allocations: Vec<CompiledProviderAllocation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompiledTechnosphereEdge {
    pub provider_idx: i32,
    pub consumer_idx: i32,
    pub flow_id: Uuid,
    pub amount: f64,
    pub provider_partition: ScopeProcessPartition,
    pub consumer_partition: ScopeProcessPartition,
    pub partition: CompiledEdgePartition,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompiledBiosphereEdge {
    pub process_idx: i32,
    pub flow_idx: i32,
    pub amount: f64,
    pub process_partition: ScopeProcessPartition,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompiledProviderOutput {
    pub flow_id: Uuid,
    pub provider_idx: i32,
    #[serde(default)]
    pub output_exchange_internal_id: Option<String>,
    #[serde(default)]
    pub output_exchange_is_reference: bool,
    #[serde(default)]
    pub output_normalized_amount: Option<f64>,
    #[serde(default)]
    pub output_allocation_state: CompiledProviderOutputAllocationState,
    #[serde(default)]
    pub eligibility: CompiledProviderCandidateEligibility,
    #[serde(default)]
    pub reference_coefficient: Option<f64>,
    #[serde(default)]
    pub reference_direction: Option<CompiledExchangeDirection>,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct CompiledReferenceStats {
    pub missing_reference: i64,
    pub invalid_reference: i64,
    pub normalized_processes: i64,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct CompiledAllocationStats {
    pub exchange_total: i64,
    pub fraction_present_count: i64,
    pub fraction_missing_count: i64,
    pub fraction_invalid_count: i64,
    #[serde(default)]
    pub legacy_empty_allocation_as_undeclared_count: i64,
    #[serde(default)]
    pub legacy_single_output_target_inferred_count: i64,
    #[serde(default)]
    pub legacy_single_reference_target_inferred_count: i64,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct CompiledMatchingStats {
    pub input_edges_total: i64,
    pub matched_unique_provider: i64,
    pub matched_multi_provider: i64,
    pub unmatched_no_provider: i64,
    pub matched_multi_resolved: i64,
    pub matched_multi_unresolved: i64,
    pub matched_multi_fallback_equal: i64,
    pub a_input_edges_written: i64,
    #[serde(default)]
    pub residual_edges_total: i64,
    #[serde(default)]
    pub a_balance_edges_written: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompiledGraph {
    pub processes: Vec<CompiledProcess>,
    pub flows: Vec<CompiledFlow>,
    #[serde(default)]
    pub reference_ports: Vec<CompiledReferencePort>,
    #[serde(default)]
    pub balance_resolutions: Vec<CompiledBalanceResolution>,
    #[serde(default)]
    pub unresolved_balances: Vec<CompiledUnresolvedBalance>,
    #[serde(default)]
    pub provider_outputs: Vec<CompiledProviderOutput>,
    pub provider_decisions: Vec<CompiledProviderDecision>,
    pub technosphere_edges: Vec<CompiledTechnosphereEdge>,
    pub biosphere_edges: Vec<CompiledBiosphereEdge>,
    pub reference_stats: CompiledReferenceStats,
    pub allocation_stats: CompiledAllocationStats,
    pub matching_stats: CompiledMatchingStats,
    /// Exact source identities required to materialize directional LCI and release datasets.
    /// Older snapshot artifacts do not contain this additive field and are intentionally not
    /// eligible for canonical Calculation Bundle generation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub release_evidence: Option<CompiledReleaseEvidence>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompiledReleaseEvidence {
    pub processes: Vec<CompiledReleaseProcess>,
    pub inventory_exchanges: Vec<CompiledReleaseInventoryExchange>,
    pub technosphere_edges: Vec<CompiledReleaseTechnosphereEdge>,
    pub biosphere_edges: Vec<CompiledReleaseInventoryExchange>,
    /// Exact canonical TIDAS documents selected while the snapshot was built.
    /// Calculation Bundle generation requires this additive evidence and never
    /// reconstructs it from mutable database state during solve execution.
    #[serde(default)]
    pub source_datasets: Vec<CompiledReleaseSourceDataset>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CompiledReleaseSourceDatasetType {
    Contact,
    Flow,
    FlowProperty,
    LciaMethod,
    Process,
    Source,
    UnitGroup,
}

impl CompiledReleaseSourceDatasetType {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Contact => "contact",
            Self::Flow => "flow",
            Self::FlowProperty => "flowproperty",
            Self::LciaMethod => "lciamethod",
            Self::Process => "process",
            Self::Source => "source",
            Self::UnitGroup => "unitgroup",
        }
    }

    #[must_use]
    pub const fn directory(self) -> &'static str {
        match self {
            Self::Contact => "contacts",
            Self::Flow => "flows",
            Self::FlowProperty => "flowproperties",
            Self::LciaMethod => "lciamethods",
            Self::Process => "processes",
            Self::Source => "sources",
            Self::UnitGroup => "unitgroups",
        }
    }

    #[must_use]
    pub const fn document_identity_keys(self) -> (&'static str, &'static str) {
        match self {
            Self::Contact => ("contactDataSet", "contactInformation"),
            Self::Flow => ("flowDataSet", "flowInformation"),
            Self::FlowProperty => ("flowPropertyDataSet", "flowPropertiesInformation"),
            Self::LciaMethod => ("LCIAMethodDataSet", "LCIAMethodInformation"),
            Self::Process => ("processDataSet", "processInformation"),
            Self::Source => ("sourceDataSet", "sourceInformation"),
            Self::UnitGroup => ("unitGroupDataSet", "unitGroupInformation"),
        }
    }

    #[must_use]
    pub fn document_uuid(self, document: &Value) -> Option<&str> {
        let (root_key, information_key) = self.document_identity_keys();
        document
            .get(root_key)?
            .get(information_key)?
            .get("dataSetInformation")?
            .get("common:UUID")?
            .as_str()
            .map(str::trim)
            .filter(|value| !value.is_empty())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompiledReleaseSourceDatasetRole {
    Support,
    UnitProcess,
}

impl CompiledReleaseSourceDatasetRole {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Support => "support",
            Self::UnitProcess => "unit_process",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompiledReleaseSourceDataset {
    pub dataset_type: CompiledReleaseSourceDatasetType,
    pub role: CompiledReleaseSourceDatasetRole,
    pub dataset_id: Uuid,
    pub dataset_version: String,
    pub document_sha256: String,
    pub document: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompiledReleaseProcess {
    pub process_idx: i32,
    pub process_id: Uuid,
    pub process_version: String,
    pub quantitative_reference_exchange_internal_id: String,
    pub quantitative_reference_flow_id: Uuid,
    pub quantitative_reference_flow_version: String,
    pub reference_unit: String,
    pub normalized_mean_amount: f64,
    #[serde(default)]
    pub reference_direction: Option<CompiledExchangeDirection>,
    #[serde(default)]
    pub raw_reference_amount: Option<f64>,
    #[serde(default)]
    pub signed_raw_reference_coefficient: Option<f64>,
    #[serde(default)]
    pub normalized_reference_coefficient: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompiledReleaseInventoryExchange {
    pub process_idx: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exchange_internal_id: Option<String>,
    pub flow_id: Uuid,
    pub flow_version: String,
    pub direction: CompiledExchangeDirection,
    pub unit: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
    pub normalized_mean_amount: f64,
    pub allocation_target_internal_id: String,
    pub allocation_fraction: f64,
    #[serde(default)]
    pub signed_normalized_coefficient: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompiledReleaseTechnosphereEdge {
    pub dependent_process_idx: i32,
    pub residual_exchange_internal_id: String,
    pub balancing_process_idx: i32,
    pub balancing_reference_exchange_internal_id: String,
    pub residual_coefficient: f64,
    pub reference_coefficient: f64,
    pub routing_weight: f64,
    pub activity_requirement: f64,
    pub flow_id: Uuid,
    pub flow_version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{CompiledAllocationStats, CompiledEdgePartition, ScopeProcessPartition};

    #[test]
    fn compiled_edge_partition_tracks_cross_partition_edges() {
        assert_eq!(
            CompiledEdgePartition::from_partitions(
                ScopeProcessPartition::Public,
                ScopeProcessPartition::Private,
            ),
            CompiledEdgePartition::PublicToPrivate
        );
        assert_eq!(
            CompiledEdgePartition::from_partitions(
                ScopeProcessPartition::Private,
                ScopeProcessPartition::Public,
            ),
            CompiledEdgePartition::PrivateToPublic
        );
    }

    #[test]
    fn legacy_allocation_stats_default_fallback_counts_to_zero() {
        let stats: CompiledAllocationStats = serde_json::from_value(json!({
            "exchange_total": 4,
            "fraction_present_count": 2,
            "fraction_missing_count": 2,
            "fraction_invalid_count": 0
        }))
        .expect("parse legacy allocation stats");

        assert_eq!(stats.legacy_empty_allocation_as_undeclared_count, 0);
        assert_eq!(stats.legacy_single_output_target_inferred_count, 0);
        assert_eq!(stats.legacy_single_reference_target_inferred_count, 0);
    }
}
