use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::graph_types::ScopeProcessPartition;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompiledFlowKind {
    Product,
    Elementary,
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
    pub kind: CompiledFlowKind,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompiledProviderDecision {
    pub consumer_idx: i32,
    pub flow_id: Uuid,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompiledGraph {
    pub processes: Vec<CompiledProcess>,
    pub flows: Vec<CompiledFlow>,
    #[serde(default)]
    pub provider_outputs: Vec<CompiledProviderOutput>,
    pub provider_decisions: Vec<CompiledProviderDecision>,
    pub technosphere_edges: Vec<CompiledTechnosphereEdge>,
    pub biosphere_edges: Vec<CompiledBiosphereEdge>,
    pub reference_stats: CompiledReferenceStats,
    pub allocation_stats: CompiledAllocationStats,
    pub matching_stats: CompiledMatchingStats,
}

#[cfg(test)]
mod tests {
    use super::{CompiledEdgePartition, ScopeProcessPartition};

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
}
