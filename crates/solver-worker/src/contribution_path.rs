use std::collections::{HashMap, HashSet};

use anyhow::Context;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use solver_core::{ModelSparseData, SolveResult};
use uuid::Uuid;

use crate::snapshot_index::SnapshotIndexDocument;

const CONTRIBUTION_PATH_FORMAT: &str = "contribution-path:v1";
const DIRECT_EPSILON: f64 = 1e-12;

/// Traversal options for contribution path analysis.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct ContributionPathOptions {
    #[serde(default = "default_max_depth")]
    pub max_depth: usize,
    #[serde(default = "default_top_k_children")]
    pub top_k_children: usize,
    #[serde(default = "default_cutoff_share")]
    pub cutoff_share: f64,
    #[serde(default = "default_max_nodes")]
    pub max_nodes: usize,
}

impl Default for ContributionPathOptions {
    fn default() -> Self {
        Self {
            max_depth: default_max_depth(),
            top_k_children: default_top_k_children(),
            cutoff_share: default_cutoff_share(),
            max_nodes: default_max_nodes(),
        }
    }
}

impl ContributionPathOptions {
    /// Normalizes user-provided values to safe runtime bounds.
    #[must_use]
    pub fn normalized(self) -> Self {
        let cutoff_share = if self.cutoff_share.is_finite() && self.cutoff_share >= 0.0 {
            self.cutoff_share.min(1.0)
        } else {
            default_cutoff_share()
        };

        Self {
            max_depth: self.max_depth.clamp(1, 8),
            top_k_children: self.top_k_children.clamp(1, 20),
            cutoff_share,
            max_nodes: self.max_nodes.clamp(10, 2_000),
        }
    }
}

fn default_max_depth() -> usize {
    4
}

fn default_top_k_children() -> usize {
    5
}

fn default_cutoff_share() -> f64 {
    0.01
}

fn default_max_nodes() -> usize {
    200
}

/// JSON artifact persisted for one contribution path analysis result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContributionPathArtifact {
    pub version: u8,
    pub format: String,
    pub snapshot_id: Uuid,
    pub job_id: Uuid,
    pub process_id: Uuid,
    pub impact_id: Uuid,
    pub amount: f64,
    pub options: ContributionPathOptions,
    pub summary: ContributionPathSummary,
    pub root: ContributionPathRoot,
    pub impact: ContributionPathImpact,
    pub process_contributions: Vec<ContributionPathProcessContribution>,
    pub branches: Vec<ContributionPathBranch>,
    pub links: Vec<ContributionPathLink>,
    pub meta: ContributionPathMeta,
}

/// Top-level summary for one path analysis result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContributionPathSummary {
    pub total_impact: f64,
    pub unit: String,
    pub coverage_ratio: f64,
    pub expanded_node_count: usize,
    pub truncated_node_count: usize,
    pub computed_at: String,
}

/// Root process descriptor.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContributionPathRoot {
    pub process_id: Uuid,
    pub label: String,
}

/// Impact descriptor.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContributionPathImpact {
    pub impact_id: Uuid,
    pub label: String,
    pub unit: String,
}

/// Exact direct contribution per process.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContributionPathProcessContribution {
    pub process_id: Uuid,
    pub process_index: i32,
    pub label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
    pub direct_impact: f64,
    pub share_of_total: f64,
    pub is_root: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub depth_min: Option<usize>,
}

/// One explored branch terminal path.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContributionPathBranch {
    pub rank: usize,
    pub path_process_ids: Vec<Uuid>,
    pub path_labels: Vec<String>,
    pub path_score: f64,
    pub terminal_reason: String,
}

/// Parent-child relation discovered during traversal.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContributionPathLink {
    pub source_process_id: Uuid,
    pub target_process_id: Uuid,
    pub depth_from_root: usize,
    pub cycle_cut: bool,
    pub direct_impact: f64,
    pub share_of_total: f64,
}

/// Metadata about how the path analysis was produced.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContributionPathMeta {
    pub source: String,
    pub snapshot_index_version: u8,
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub fn analyze_contribution_path(
    snapshot_id: Uuid,
    job_id: Uuid,
    process_id: Uuid,
    impact_id: Uuid,
    process_index: i32,
    impact_index: i32,
    amount: f64,
    options: ContributionPathOptions,
    snapshot_index: &SnapshotIndexDocument,
    data: &ModelSparseData,
    solved: &SolveResult,
) -> anyhow::Result<ContributionPathArtifact> {
    let normalized = options.normalized();
    let x = solved
        .x
        .as_ref()
        .context("contribution path requires solve_one x payload")?;
    let h = solved
        .h
        .as_ref()
        .context("contribution path requires solve_one h payload")?;

    let root_process_index = usize::try_from(process_index)
        .map_err(|_| anyhow::anyhow!("process_index overflow: {process_index}"))?;
    let target_impact_index = usize::try_from(impact_index)
        .map_err(|_| anyhow::anyhow!("impact_index overflow: {impact_index}"))?;

    if x.len()
        != usize::try_from(data.process_count)
            .map_err(|_| anyhow::anyhow!("process_count overflow: {}", data.process_count))?
    {
        return Err(anyhow::anyhow!(
            "x length mismatch: x_len={} process_count={}",
            x.len(),
            data.process_count
        ));
    }
    if target_impact_index >= h.len() {
        return Err(anyhow::anyhow!(
            "impact_index out of solve h bounds: impact_index={} h_len={}",
            impact_index,
            h.len()
        ));
    }
    if root_process_index >= x.len() {
        return Err(anyhow::anyhow!(
            "process_index out of solve x bounds: process_index={} x_len={}",
            process_index,
            x.len()
        ));
    }

    let root_process = snapshot_index
        .process_map
        .iter()
        .find(|entry| entry.process_index == process_index)
        .ok_or_else(|| anyhow::anyhow!("root process missing in snapshot index"))?;
    let impact = snapshot_index
        .impact_map
        .iter()
        .find(|entry| entry.impact_index == impact_index)
        .ok_or_else(|| anyhow::anyhow!("impact missing in snapshot index"))?;

    let mut cf_by_flow = HashMap::<usize, f64>::new();
    for cf in &data.characterization_factors {
        if cf.row == impact_index {
            let flow_idx = usize::try_from(cf.col)
                .map_err(|_| anyhow::anyhow!("flow index overflow: {}", cf.col))?;
            *cf_by_flow.entry(flow_idx).or_insert(0.0) += cf.value;
        }
    }

    let mut direct_intensity_by_process = vec![0.0_f64; x.len()];
    for triplet in &data.biosphere_entries {
        let flow_idx = usize::try_from(triplet.row)
            .map_err(|_| anyhow::anyhow!("biosphere flow index overflow: {}", triplet.row))?;
        let Some(cf_value) = cf_by_flow.get(&flow_idx).copied() else {
            continue;
        };
        let process_idx = usize::try_from(triplet.col)
            .map_err(|_| anyhow::anyhow!("biosphere process index overflow: {}", triplet.col))?;
        if process_idx >= direct_intensity_by_process.len() {
            return Err(anyhow::anyhow!(
                "biosphere process index out of range: process_idx={process_idx} len={}",
                direct_intensity_by_process.len()
            ));
        }
        direct_intensity_by_process[process_idx] += cf_value * triplet.value;
    }

    let mut direct_contributions = vec![0.0_f64; x.len()];
    for (idx, activity) in x.iter().copied().enumerate() {
        direct_contributions[idx] = activity * direct_intensity_by_process[idx];
    }

    let total_impact = h[target_impact_index];
    let total_abs_contribution = direct_contributions
        .iter()
        .map(|value| value.abs())
        .sum::<f64>()
        .max(DIRECT_EPSILON);

    let process_meta_by_index = snapshot_index
        .process_map
        .iter()
        .map(|entry| (entry.process_index, entry))
        .collect::<HashMap<_, _>>();

    let adjacency = build_provider_adjacency(data)?;
    let traversal = traverse_upstream(
        process_index,
        &adjacency,
        &direct_contributions,
        total_abs_contribution,
        &process_meta_by_index,
        normalized,
    )?;

    let mut process_contributions = snapshot_index
        .process_map
        .iter()
        .filter_map(|entry| {
            let idx = usize::try_from(entry.process_index).ok()?;
            let direct_impact = *direct_contributions.get(idx)?;
            if direct_impact.abs() <= DIRECT_EPSILON && entry.process_index != process_index {
                return None;
            }

            Some(ContributionPathProcessContribution {
                process_id: entry.process_id,
                process_index: entry.process_index,
                label: entry
                    .process_name
                    .clone()
                    .unwrap_or_else(|| entry.process_id.to_string()),
                location: entry.location.clone(),
                direct_impact,
                share_of_total: direct_impact.abs() / total_abs_contribution,
                is_root: entry.process_index == process_index,
                depth_min: traversal.min_depth.get(&entry.process_index).copied(),
            })
        })
        .collect::<Vec<_>>();
    process_contributions.sort_by(|left, right| {
        right
            .direct_impact
            .abs()
            .total_cmp(&left.direct_impact.abs())
            .then_with(|| left.label.cmp(&right.label))
    });

    let covered_abs = traversal
        .expanded_nodes
        .iter()
        .filter_map(|idx| usize::try_from(*idx).ok())
        .filter_map(|idx| direct_contributions.get(idx))
        .map(|value| value.abs())
        .sum::<f64>();
    let coverage_ratio = (covered_abs / total_abs_contribution).clamp(0.0, 1.0);

    Ok(ContributionPathArtifact {
        version: 1,
        format: CONTRIBUTION_PATH_FORMAT.to_owned(),
        snapshot_id,
        job_id,
        process_id,
        impact_id,
        amount,
        options: normalized,
        summary: ContributionPathSummary {
            total_impact,
            unit: impact.unit.clone(),
            coverage_ratio,
            expanded_node_count: traversal.expanded_nodes.len(),
            truncated_node_count: traversal.truncated_node_count,
            computed_at: Utc::now().to_rfc3339(),
        },
        root: ContributionPathRoot {
            process_id: root_process.process_id,
            label: root_process
                .process_name
                .clone()
                .unwrap_or_else(|| root_process.process_id.to_string()),
        },
        impact: ContributionPathImpact {
            impact_id: impact.impact_id,
            label: impact.impact_name.clone(),
            unit: impact.unit.clone(),
        },
        process_contributions,
        branches: traversal.branches,
        links: traversal.links,
        meta: ContributionPathMeta {
            source: "solve_one_path_analysis".to_owned(),
            snapshot_index_version: snapshot_index.version,
        },
    })
}

type ProviderAdjacency = Vec<Vec<i32>>;

fn build_provider_adjacency(data: &ModelSparseData) -> anyhow::Result<ProviderAdjacency> {
    let process_count = usize::try_from(data.process_count)
        .map_err(|_| anyhow::anyhow!("process_count overflow: {}", data.process_count))?;
    let mut adjacency = vec![Vec::<i32>::new(); process_count];

    for triplet in &data.technosphere_entries {
        let provider_idx = usize::try_from(triplet.row)
            .map_err(|_| anyhow::anyhow!("provider index overflow: {}", triplet.row))?;
        let consumer_idx = usize::try_from(triplet.col)
            .map_err(|_| anyhow::anyhow!("consumer index overflow: {}", triplet.col))?;
        if provider_idx >= process_count || consumer_idx >= process_count {
            return Err(anyhow::anyhow!(
                "technosphere index out of bounds: provider_idx={provider_idx} consumer_idx={consumer_idx} process_count={process_count}"
            ));
        }
        adjacency[consumer_idx].push(triplet.row);
    }

    for providers in &mut adjacency {
        providers.sort_unstable();
        providers.dedup();
    }

    Ok(adjacency)
}

#[derive(Debug)]
struct TraversalOutput {
    expanded_nodes: HashSet<i32>,
    min_depth: HashMap<i32, usize>,
    truncated_node_count: usize,
    branches: Vec<ContributionPathBranch>,
    links: Vec<ContributionPathLink>,
}

fn traverse_upstream(
    root_process_index: i32,
    adjacency: &ProviderAdjacency,
    direct_contributions: &[f64],
    total_abs_contribution: f64,
    process_meta_by_index: &HashMap<i32, &crate::snapshot_index::SnapshotProcessMapEntry>,
    options: ContributionPathOptions,
) -> anyhow::Result<TraversalOutput> {
    let mut state = TraversalState {
        adjacency,
        direct_contributions,
        total_abs_contribution,
        process_meta_by_index,
        options,
        expanded_nodes: HashSet::from([root_process_index]),
        min_depth: HashMap::from([(root_process_index, 0)]),
        truncated_node_count: 0,
        branches: Vec::new(),
        links: Vec::new(),
    };

    let mut path = vec![root_process_index];
    state.walk(root_process_index, 0, &mut path)?;
    state.branches.sort_by(|left, right| {
        right.path_score.total_cmp(&left.path_score).then_with(|| {
            left.path_labels
                .join(" > ")
                .cmp(&right.path_labels.join(" > "))
        })
    });
    for (rank, branch) in state.branches.iter_mut().enumerate() {
        branch.rank = rank + 1;
    }

    Ok(TraversalOutput {
        expanded_nodes: state.expanded_nodes,
        min_depth: state.min_depth,
        truncated_node_count: state.truncated_node_count,
        branches: state.branches,
        links: state.links,
    })
}

struct TraversalState<'a> {
    adjacency: &'a ProviderAdjacency,
    direct_contributions: &'a [f64],
    total_abs_contribution: f64,
    process_meta_by_index: &'a HashMap<i32, &'a crate::snapshot_index::SnapshotProcessMapEntry>,
    options: ContributionPathOptions,
    expanded_nodes: HashSet<i32>,
    min_depth: HashMap<i32, usize>,
    truncated_node_count: usize,
    branches: Vec<ContributionPathBranch>,
    links: Vec<ContributionPathLink>,
}

impl TraversalState<'_> {
    fn walk(
        &mut self,
        current_process_index: i32,
        depth: usize,
        path: &mut Vec<i32>,
    ) -> anyhow::Result<()> {
        if depth >= self.options.max_depth {
            self.truncated_node_count += 1;
            self.record_branch(path, "max_depth")?;
            return Ok(());
        }

        let providers = self
            .adjacency
            .get(usize::try_from(current_process_index).map_err(|_| {
                anyhow::anyhow!("current process index overflow: {current_process_index}")
            })?)
            .ok_or_else(|| anyhow::anyhow!("current process index missing from adjacency"))?;

        if providers.is_empty() {
            self.record_branch(path, "leaf")?;
            return Ok(());
        }

        let mut ranked = providers
            .iter()
            .filter_map(|provider_idx| {
                let idx = usize::try_from(*provider_idx).ok()?;
                let direct = *self.direct_contributions.get(idx)?;
                let share = direct.abs() / self.total_abs_contribution;
                Some((*provider_idx, direct, share))
            })
            .filter(|(_, _, share)| *share >= self.options.cutoff_share)
            .collect::<Vec<_>>();

        if ranked.is_empty() {
            self.truncated_node_count += providers.len();
            self.record_branch(path, "cutoff")?;
            return Ok(());
        }

        ranked.sort_by(|left, right| {
            right
                .1
                .abs()
                .total_cmp(&left.1.abs())
                .then_with(|| left.0.cmp(&right.0))
        });

        let mut expanded_any = false;
        for (position, (provider_idx, direct_impact, share_of_total)) in
            ranked.iter().copied().enumerate()
        {
            if position >= self.options.top_k_children {
                self.truncated_node_count += 1;
                continue;
            }

            let target_meta = self
                .process_meta_by_index
                .get(&provider_idx)
                .ok_or_else(|| {
                    anyhow::anyhow!("provider {provider_idx} missing from snapshot index")
                })?;

            if path.contains(&provider_idx) {
                self.links.push(ContributionPathLink {
                    source_process_id: self.process_id_for_index(current_process_index)?,
                    target_process_id: target_meta.process_id,
                    depth_from_root: depth + 1,
                    cycle_cut: true,
                    direct_impact,
                    share_of_total,
                });
                path.push(provider_idx);
                self.truncated_node_count += 1;
                self.record_branch(path, "cycle_cut")?;
                path.pop();
                expanded_any = true;
                continue;
            }

            if !self.expanded_nodes.contains(&provider_idx)
                && self.expanded_nodes.len() >= self.options.max_nodes
            {
                path.push(provider_idx);
                self.truncated_node_count += 1;
                self.record_branch(path, "max_nodes")?;
                path.pop();
                expanded_any = true;
                continue;
            }

            self.expanded_nodes.insert(provider_idx);
            self.min_depth
                .entry(provider_idx)
                .and_modify(|existing| *existing = (*existing).min(depth + 1))
                .or_insert(depth + 1);
            self.links.push(ContributionPathLink {
                source_process_id: self.process_id_for_index(current_process_index)?,
                target_process_id: target_meta.process_id,
                depth_from_root: depth + 1,
                cycle_cut: false,
                direct_impact,
                share_of_total,
            });
            path.push(provider_idx);
            self.walk(provider_idx, depth + 1, path)?;
            path.pop();
            expanded_any = true;
        }

        if !expanded_any {
            self.record_branch(path, "top_k")?;
        }

        Ok(())
    }

    fn record_branch(&mut self, path: &[i32], terminal_reason: &str) -> anyhow::Result<()> {
        let path_process_ids = path
            .iter()
            .map(|idx| self.process_id_for_index(*idx))
            .collect::<anyhow::Result<Vec<_>>>()?;
        let path_labels = path
            .iter()
            .map(|idx| self.label_for_index(*idx))
            .collect::<anyhow::Result<Vec<_>>>()?;
        let path_score = path
            .iter()
            .skip(1)
            .filter_map(|idx| usize::try_from(*idx).ok())
            .filter_map(|idx| self.direct_contributions.get(idx))
            .map(|value| value.abs())
            .sum::<f64>();

        self.branches.push(ContributionPathBranch {
            rank: 0,
            path_process_ids,
            path_labels,
            path_score,
            terminal_reason: terminal_reason.to_owned(),
        });
        Ok(())
    }

    fn process_id_for_index(&self, process_index: i32) -> anyhow::Result<Uuid> {
        self.process_meta_by_index
            .get(&process_index)
            .map(|entry| entry.process_id)
            .ok_or_else(|| {
                anyhow::anyhow!("process_index missing from snapshot index: {process_index}")
            })
    }

    fn label_for_index(&self, process_index: i32) -> anyhow::Result<String> {
        let entry = self
            .process_meta_by_index
            .get(&process_index)
            .ok_or_else(|| {
                anyhow::anyhow!("process_index missing from snapshot index: {process_index}")
            })?;
        Ok(entry
            .process_name
            .clone()
            .unwrap_or_else(|| entry.process_id.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use solver_core::{FactorizationState, SolveResult, SparseTriplet};

    use super::*;
    use crate::snapshot_index::{SnapshotImpactMapEntry, SnapshotProcessMapEntry};

    #[allow(clippy::too_many_lines)]
    #[test]
    fn analyze_returns_sorted_process_contributions_and_branches() {
        let snapshot_id = Uuid::new_v4();
        let process_a = Uuid::new_v4();
        let process_b = Uuid::new_v4();
        let process_c = Uuid::new_v4();
        let impact_id = Uuid::new_v4();

        let snapshot_index = SnapshotIndexDocument {
            version: 1,
            snapshot_id,
            process_count: 3,
            impact_count: 1,
            process_map: vec![
                SnapshotProcessMapEntry {
                    process_id: process_a,
                    process_index: 0,
                    process_version: "01.00.000".to_owned(),
                    process_name: Some("Root".to_owned()),
                    location: Some("CN".to_owned()),
                },
                SnapshotProcessMapEntry {
                    process_id: process_b,
                    process_index: 1,
                    process_version: "01.00.000".to_owned(),
                    process_name: Some("Provider B".to_owned()),
                    location: Some("CN".to_owned()),
                },
                SnapshotProcessMapEntry {
                    process_id: process_c,
                    process_index: 2,
                    process_version: "01.00.000".to_owned(),
                    process_name: Some("Provider C".to_owned()),
                    location: Some("GLO".to_owned()),
                },
            ],
            impact_map: vec![SnapshotImpactMapEntry {
                impact_id,
                impact_index: 0,
                impact_key: "GWP".to_owned(),
                impact_name: "Global warming".to_owned(),
                unit: "kg CO2-eq".to_owned(),
            }],
            calculation_evidence: None,
        };

        let data = ModelSparseData {
            model_version: snapshot_id,
            process_count: 3,
            flow_count: 1,
            impact_count: 1,
            technosphere_entries: vec![
                SparseTriplet {
                    row: 1,
                    col: 0,
                    value: 0.5,
                },
                SparseTriplet {
                    row: 2,
                    col: 1,
                    value: 0.25,
                },
            ],
            biosphere_entries: vec![
                SparseTriplet {
                    row: 0,
                    col: 0,
                    value: 1.0,
                },
                SparseTriplet {
                    row: 0,
                    col: 1,
                    value: 2.0,
                },
                SparseTriplet {
                    row: 0,
                    col: 2,
                    value: 4.0,
                },
            ],
            characterization_factors: vec![SparseTriplet {
                row: 0,
                col: 0,
                value: 1.5,
            }],
        };

        let solved = SolveResult {
            x: Some(vec![1.0, 0.5, 0.125]),
            g: Some(vec![2.5]),
            h: Some(vec![2.25]),
            factorization_state: FactorizationState::Ready,
        };

        let artifact = analyze_contribution_path(
            snapshot_id,
            Uuid::new_v4(),
            process_a,
            impact_id,
            0,
            0,
            1.0,
            ContributionPathOptions::default(),
            &snapshot_index,
            &data,
            &solved,
        )
        .expect("analyze contribution path");

        assert!((artifact.summary.total_impact - 2.25).abs() < 1e-12);
        assert_eq!(artifact.root.label, "Root");
        assert_eq!(artifact.impact.label, "Global warming");
        assert_eq!(artifact.process_contributions.len(), 3);
        assert_eq!(artifact.process_contributions[0].label, "Provider B");
        assert_eq!(artifact.links.len(), 2);
        assert_eq!(artifact.branches.len(), 1);
        assert_eq!(
            artifact.branches[0].path_labels,
            vec!["Root", "Provider B", "Provider C"]
        );
    }
}
