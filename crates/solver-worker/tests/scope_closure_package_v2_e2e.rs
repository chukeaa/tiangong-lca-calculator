//! Real local-stack lifecycle proof for scope closure -> certified snapshot -> Build V2.
//!
//! Run only through `scripts/run_scope_closure_package_v2_e2e.sh`. The harness resets a local
//! Supabase database, uses its S3-compatible Storage endpoint, builds the real snapshot-builder
//! binary, and supplies a deterministic TIDAS process-protocol seam. The seam deliberately does
//! not claim to validate TIDAS semantics; every database, Worker, snapshot, HDF5, solve, package,
//! object-storage, tamper, and revocation boundary exercised below is real.

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use clap::Parser;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use solver_worker::{
    calculation_evidence::RELEASE_METHOD_IDENTITIES,
    config::AppConfig,
    db::AppState,
    queue::run_solver_worker_jobs_loop,
    snapshot_artifacts::{
        SNAPSHOT_ARTIFACT_CONTENT_TYPE, SNAPSHOT_ARTIFACT_FORMAT, decode_snapshot_artifact,
    },
    snapshot_index::{SnapshotIndexDocument, derive_snapshot_index_url},
    worker_jobs::{WorkerJobResult, claim_worker_jobs, record_worker_job_result},
};
use sqlx::{PgPool, Row};
use tokio::{task::JoinHandle, time::sleep};
use uuid::Uuid;

const VERSION: &str = "01.00.000";

#[derive(Debug)]
struct Fixture {
    actor: Uuid,
    processes: [Uuid; 2],
    product_flows: [Uuid; 2],
    elementary_flow: Uuid,
    flow_property: Uuid,
    unit_group: Uuid,
}

#[derive(Debug)]
struct Certificate {
    check_id: Uuid,
    requested_scope_hash: String,
    policy_fingerprint: String,
    snapshot_id: Uuid,
    snapshot_hash: String,
    snapshot_artifact_id: Uuid,
    snapshot_index_sha256: String,
    snapshot_build_contract_hash: String,
    effective_scope_hash: String,
    data_snapshot_token: String,
    closure_bundle_hash: String,
    artifact_url: String,
    effective_scope: Value,
}

#[derive(Debug)]
struct Build {
    build_id: Uuid,
    worker_job_id: Uuid,
}

fn required_env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| {
        panic!(
            "{name} is required; run scripts/run_scope_closure_package_v2_e2e.sh from the Worker repo"
        )
    })
}

fn test_config() -> AppConfig {
    AppConfig::parse_from([
        "solver-worker-e2e",
        "--database-url",
        required_env("DATABASE_URL").as_str(),
        "--s3-endpoint",
        required_env("S3_ENDPOINT").as_str(),
        "--s3-region",
        required_env("S3_REGION").as_str(),
        "--s3-bucket",
        required_env("S3_BUCKET").as_str(),
        "--s3-access-key-id",
        required_env("S3_ACCESS_KEY_ID").as_str(),
        "--s3-secret-access-key",
        required_env("S3_SECRET_ACCESS_KEY").as_str(),
        "--s3-prefix",
        "scope-closure-package-v2-e2e",
        "--db-max-connections",
        "12",
        "--worker-poll-ms",
        "20",
    ])
}

fn sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn repeated(character: char) -> String {
    std::iter::repeat_n(character, 64).collect()
}

fn start_worker(state: Arc<AppState>, label: &str) -> JoinHandle<anyhow::Result<()>> {
    tokio::spawn(run_solver_worker_jobs_loop(
        state,
        format!("scope-closure-package-v2-e2e-{label}"),
        1,
        300,
        Duration::from_millis(20),
    ))
}

async fn stop_worker(worker: JoinHandle<anyhow::Result<()>>) {
    worker.abort();
    let _ = worker.await;
}

async fn wait_for_job(pool: &PgPool, job_id: Uuid) -> anyhow::Result<String> {
    tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            let row = sqlx::query(
                "SELECT status, error_code, error_message FROM public.worker_jobs WHERE id=$1",
            )
            .bind(job_id)
            .fetch_one(pool)
            .await?;
            let status = row.try_get::<String, _>("status")?;
            if matches!(
                status.as_str(),
                "completed" | "blocked" | "failed" | "dead_letter" | "cancelled"
            ) {
                if status != "completed" {
                    eprintln!(
                        "job {job_id} terminal status={status} code={:?} message={:?}",
                        row.try_get::<Option<String>, _>("error_code")?,
                        row.try_get::<Option<String>, _>("error_message")?
                    );
                }
                return Ok(status);
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("timed out after 90s waiting for worker job {job_id}"))?
}

async fn run_one_job(state: Arc<AppState>, job_id: Uuid, label: &str) -> anyhow::Result<String> {
    let worker = start_worker(state.clone(), label);
    let status = wait_for_job(&state.pool, job_id).await;
    stop_worker(worker).await;
    status
}

fn process_document(
    process: Uuid,
    product: Uuid,
    elementary: Uuid,
    amount: f64,
    input_product: Option<Uuid>,
) -> Value {
    let mut exchanges = vec![
        json!({
            "@dataSetInternalID": "1",
            "exchangeDirection": "Output",
            "resultingAmount": "1",
            "referenceToFlowDataSet": {
                "@type": "flow data set",
                "@refObjectId": product,
                "@version": VERSION
            }
        }),
        json!({
            "@dataSetInternalID": "2",
            "exchangeDirection": "Output",
            "resultingAmount": amount.to_string(),
            "referenceToFlowDataSet": {
                "@type": "flow data set",
                "@refObjectId": elementary,
                "@version": VERSION
            }
        }),
    ];
    if let Some(input_product) = input_product {
        exchanges.push(json!({
            "@dataSetInternalID": "3",
            "exchangeDirection": "Input",
            "resultingAmount": "0.2",
            "referenceToFlowDataSet": {
                "@type": "flow data set",
                "@refObjectId": input_product,
                "@version": VERSION
            }
        }));
    }
    json!({
        "processDataSet": {
            "processInformation": {
                "dataSetInformation": {
                    "common:UUID": process,
                    "name": {"baseName": format!("E2E process {process}")}
                },
                "quantitativeReference": {"referenceToReferenceFlow": "1"}
            },
            "exchanges": {"exchange": exchanges}
        }
    })
}

fn flow_document(flow: Uuid, flow_type: &str, flow_property: Uuid) -> Value {
    json!({
        "flowDataSet": {
            "flowInformation": {
                "dataSetInformation": {
                    "common:UUID": flow,
                    "name": {"baseName": format!("E2E flow {flow}")}
                },
                "quantitativeReference": {"referenceToReferenceFlowProperty": "1"}
            },
            "flowProperties": {"flowProperty": {
                "@dataSetInternalID": "1",
                "referenceToFlowPropertyDataSet": {
                    "@type": "flow property data set",
                    "@refObjectId": flow_property,
                    "@version": VERSION
                }
            }},
            "modellingAndValidation": {"LCIMethod": {"typeOfDataSet": flow_type}}
        }
    })
}

fn flow_property_document(flow_property: Uuid, unit_group: Uuid) -> Value {
    json!({
        "flowPropertyDataSet": {
            "flowPropertiesInformation": {
                "dataSetInformation": {"common:UUID": flow_property},
                "quantitativeReference": {"referenceToReferenceUnitGroup": {
                    "@type": "unit group data set",
                    "@refObjectId": unit_group,
                    "@version": VERSION
                }}
            }
        }
    })
}

fn unit_group_document(unit_group: Uuid) -> Value {
    json!({
        "unitGroupDataSet": {
            "unitGroupInformation": {
                "dataSetInformation": {"common:UUID": unit_group},
                "quantitativeReference": {"referenceToReferenceUnit": "1"}
            },
            "units": {"unit": {
                "@dataSetInternalID": "1",
                "name": "kg",
                "meanValue": "1"
            }}
        }
    })
}

fn method_document(method: Uuid, elementary: Uuid) -> Value {
    json!({
        "LCIAMethodDataSet": {
            "LCIAMethodInformation": {
                "dataSetInformation": {"common:UUID": method}
            },
            "methodInformation": {
                "dataSetInformation": {"name": {"baseName": "E2E impact"}}
            },
            "characterisationFactors": {"factor": {
                "referenceToFlowDataSet": {
                    "@type": "flow data set",
                    "@refObjectId": elementary,
                    "@version": VERSION
                },
                "meanValue": "2"
            }}
        }
    })
}

async fn setup_fixture(pool: &PgPool) -> anyhow::Result<Fixture> {
    let fixture = Fixture {
        actor: Uuid::new_v4(),
        processes: [Uuid::new_v4(), Uuid::new_v4()],
        product_flows: [Uuid::new_v4(), Uuid::new_v4()],
        elementary_flow: Uuid::new_v4(),
        flow_property: Uuid::new_v4(),
        unit_group: Uuid::new_v4(),
    };
    let release_run = Uuid::new_v4();
    let approval = Uuid::new_v4();

    sqlx::query(
        r#"INSERT INTO storage.buckets(id,name,public,file_size_limit,allowed_mime_types)
           VALUES($1,$1,false,52428800,ARRAY['application/x-hdf5','application/json',
             'application/x-ndjson','application/gzip',
             'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'])
           ON CONFLICT(id) DO NOTHING"#,
    )
    .bind(required_env("S3_BUCKET"))
    .execute(pool)
    .await?;
    sqlx::query(
        r#"INSERT INTO auth.users(instance_id,id,aud,role,email,encrypted_password,email_confirmed_at,
             raw_app_meta_data,raw_user_meta_data,created_at,updated_at,is_sso_user,is_anonymous)
           VALUES('00000000-0000-0000-0000-000000000000',$1,'authenticated','authenticated',$2,
             'x',now(),'{}','{}',now(),now(),false,false)"#,
    )
    .bind(fixture.actor)
    .bind(format!("scope-closure-e2e-{}@example.com", fixture.actor))
    .execute(pool)
    .await?;
    sqlx::query("INSERT INTO public.users(id,raw_user_meta_data,contact) VALUES($1,'{}',null)")
        .bind(fixture.actor)
        .execute(pool)
        .await?;
    sqlx::query(
        "INSERT INTO public.teams(id,json,rank,is_public) VALUES('00000000-0000-0000-0000-000000000000','{\"name\":\"System\"}',0,false) ON CONFLICT(id) DO NOTHING",
    )
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO public.roles(user_id,team_id,role) VALUES($1,'00000000-0000-0000-0000-000000000000','data_product_manager')",
    )
    .bind(fixture.actor)
    .execute(pool)
    .await?;

    sqlx::query("ALTER TABLE public.processes DISABLE TRIGGER USER")
        .execute(pool)
        .await?;
    sqlx::query("ALTER TABLE public.flows DISABLE TRIGGER USER")
        .execute(pool)
        .await?;
    sqlx::query("ALTER TABLE public.flowproperties DISABLE TRIGGER USER")
        .execute(pool)
        .await?;
    sqlx::query("ALTER TABLE public.unitgroups DISABLE TRIGGER USER")
        .execute(pool)
        .await?;
    sqlx::query("ALTER TABLE public.lciamethods DISABLE TRIGGER USER")
        .execute(pool)
        .await?;
    let processes = [
        process_document(
            fixture.processes[0],
            fixture.product_flows[0],
            fixture.elementary_flow,
            3.0,
            None,
        ),
        process_document(
            fixture.processes[1],
            fixture.product_flows[1],
            fixture.elementary_flow,
            5.0,
            Some(fixture.product_flows[0]),
        ),
    ];
    for (id, document) in fixture.processes.iter().zip(&processes) {
        sqlx::query(
            "INSERT INTO public.processes(id,version,json,json_ordered,user_id,state_code) VALUES($1,$2,$3,$3::text::json,$4,100)",
        )
        .bind(id)
        .bind(VERSION)
        .bind(document)
        .bind(fixture.actor)
        .execute(pool)
        .await?;
    }

    let flows = [
        (
            fixture.product_flows[0],
            flow_document(
                fixture.product_flows[0],
                "Product flow",
                fixture.flow_property,
            ),
        ),
        (
            fixture.product_flows[1],
            flow_document(
                fixture.product_flows[1],
                "Product flow",
                fixture.flow_property,
            ),
        ),
        (
            fixture.elementary_flow,
            flow_document(
                fixture.elementary_flow,
                "Elementary flow",
                fixture.flow_property,
            ),
        ),
    ];
    for (id, document) in &flows {
        sqlx::query(
            "INSERT INTO public.flows(id,version,json,json_ordered,user_id,state_code) VALUES($1,$2,$3,$3::text::json,$4,100)",
        )
        .bind(id)
        .bind(VERSION)
        .bind(document)
        .bind(fixture.actor)
        .execute(pool)
        .await?;
    }
    let flow_property = flow_property_document(fixture.flow_property, fixture.unit_group);
    sqlx::query(
        "INSERT INTO public.flowproperties(id,version,json,json_ordered,user_id,state_code) VALUES($1,$2,$3,$3::text::json,$4,100)",
    )
    .bind(fixture.flow_property)
    .bind(VERSION)
    .bind(&flow_property)
    .bind(fixture.actor)
    .execute(pool)
    .await?;
    let unit_group = unit_group_document(fixture.unit_group);
    sqlx::query(
        "INSERT INTO public.unitgroups(id,version,json,json_ordered,user_id,state_code) VALUES($1,$2,$3,$3::text::json,$4,100)",
    )
    .bind(fixture.unit_group)
    .bind(VERSION)
    .bind(&unit_group)
    .bind(fixture.actor)
    .execute(pool)
    .await?;
    let mut methods = Vec::with_capacity(RELEASE_METHOD_IDENTITIES.len());
    for (method_id, version, locator_id) in RELEASE_METHOD_IDENTITIES {
        let method_id = Uuid::parse_str(method_id)?;
        let locator_id = Uuid::parse_str(locator_id)?;
        let method = method_document(method_id, fixture.elementary_flow);
        sqlx::query(
            "INSERT INTO public.lciamethods(id,version,json,json_ordered,user_id,state_code) VALUES($1,$2,$3,$3::text::json,$4,100)",
        )
        .bind(locator_id)
        .bind(version)
        .bind(&method)
        .bind(fixture.actor)
        .execute(pool)
        .await?;
        methods.push((method_id, version, method));
    }
    sqlx::query("ALTER TABLE public.processes ENABLE TRIGGER USER")
        .execute(pool)
        .await?;
    sqlx::query("ALTER TABLE public.flows ENABLE TRIGGER USER")
        .execute(pool)
        .await?;
    sqlx::query("ALTER TABLE public.flowproperties ENABLE TRIGGER USER")
        .execute(pool)
        .await?;
    sqlx::query("ALTER TABLE public.unitgroups ENABLE TRIGGER USER")
        .execute(pool)
        .await?;
    sqlx::query("ALTER TABLE public.lciamethods ENABLE TRIGGER USER")
        .execute(pool)
        .await?;

    sqlx::query(
        r#"INSERT INTO public.lca_release_runs(
             id,release_version,selection_manifest_hash,input_manifest_hash,calculation_bundle_hash,
             calculation_bundle_ref,profile_lock_hash,publish_plan_hash,publish_plan,artifact_set_hash,
             release_manifest_hash,release_manifest,status,idempotency_key,request_hash,created_by)
           VALUES($1,$2,$3,$4,$5,'{}',$6,$7,'{}',$8,$9,'{}','published',$10,$11,$12)"#,
    )
    .bind(release_run)
    .bind("77.00.001")
    .bind(repeated('a'))
    .bind(repeated('b'))
    .bind(repeated('c'))
    .bind(repeated('d'))
    .bind(repeated('e'))
    .bind(repeated('f'))
    .bind(repeated('9'))
    .bind(format!("scope-closure-e2e-{release_run}"))
    .bind(repeated('1'))
    .bind(fixture.actor)
    .execute(pool)
    .await?;
    sqlx::query(
        r#"INSERT INTO public.lca_release_approvals(
             id,release_run_id,publish_plan_hash,approval_hash,approved_by,approved_at,expires_at)
           VALUES($1,$2,$3,$4,$5,now(),now()+interval '1 day')"#,
    )
    .bind(approval)
    .bind(release_run)
    .bind(repeated('e'))
    .bind(repeated('2'))
    .bind(fixture.actor)
    .execute(pool)
    .await?;
    sqlx::query(
        r#"INSERT INTO public.lca_release_publications(
             release_run_id,release_version,approval_id,approval_hash,publish_plan_hash,
             release_manifest_hash,artifact_set_hash,approved_by,executed_by,credential_fingerprint,
             idempotency_key,published_at)
           VALUES($1,(SELECT release_version FROM public.lca_release_runs WHERE id=$1),$2,$3,$4,$5,$6,$7,$7,$8,$9,now())"#,
    )
    .bind(release_run)
    .bind(approval)
    .bind(repeated('2'))
    .bind(repeated('e'))
    .bind(repeated('9'))
    .bind(repeated('f'))
    .bind(fixture.actor)
    .bind(repeated('3'))
    .bind(format!("scope-closure-e2e-publication-{release_run}"))
    .execute(pool)
    .await?;

    let mut released = vec![
        (
            "process",
            "unit_process",
            fixture.processes[0],
            processes[0].clone(),
            Some(fixture.processes[0]),
            VERSION,
        ),
        (
            "process",
            "unit_process",
            fixture.processes[1],
            processes[1].clone(),
            Some(fixture.processes[1]),
            VERSION,
        ),
        (
            "flow",
            "support",
            flows[0].0,
            flows[0].1.clone(),
            None,
            VERSION,
        ),
        (
            "flow",
            "support",
            flows[1].0,
            flows[1].1.clone(),
            None,
            VERSION,
        ),
        (
            "flow",
            "support",
            flows[2].0,
            flows[2].1.clone(),
            None,
            VERSION,
        ),
        (
            "flowproperty",
            "support",
            fixture.flow_property,
            flow_property,
            None,
            VERSION,
        ),
        (
            "unitgroup",
            "support",
            fixture.unit_group,
            unit_group,
            None,
            VERSION,
        ),
    ];
    released.extend(
        methods
            .into_iter()
            .map(|(id, version, document)| ("lciamethod", "support", id, document, None, version)),
    );
    for (dataset_type, role, id, document, source_process, version) in released.drain(..) {
        let canonical_hash = solver_worker::scope_closure::canonical_json_sha256(&document)?;
        sqlx::query(
            r#"INSERT INTO public.lca_release_dataset_versions(
                 release_run_id,dataset_type,dataset_role,dataset_uuid,dataset_version,
                 source_process_uuid,source_process_version,version_significant_hash,semantic_hash,
                 canonical_content_hash,artifact_ref)
               VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'{}')"#,
        )
        .bind(release_run)
        .bind(dataset_type)
        .bind(role)
        .bind(id)
        .bind(version)
        .bind(source_process)
        .bind(source_process.map(|_| VERSION))
        .bind(repeated('4'))
        .bind(repeated('5'))
        .bind(canonical_hash)
        .execute(pool)
        .await?;
    }
    Ok(fixture)
}

async fn request_closure(pool: &PgPool, fixture: &Fixture) -> anyhow::Result<(Uuid, Uuid)> {
    let scope = json!({
        "coverageMode": "subset",
        "certificateFreshnessPolicy": "frozen-artifact-reusable-v1",
        "processes": fixture.processes.iter().map(|id| json!({"id": id, "version": VERSION})).collect::<Vec<_>>(),
        "lciaMethods": RELEASE_METHOD_IDENTITIES.iter().map(|(id, version, _)| json!({
            "id": id,
            "version": version
        })).collect::<Vec<_>>(),
    });
    let row = sqlx::query(
        r#"WITH _claims AS (
             SELECT set_config('request.jwt.claim.role','authenticated',true),
                    set_config('request.jwt.claim.sub',$3,true)
           )
           SELECT public.cmd_lcia_scope_closure_check_request_v2($1,$2,'{}'::jsonb) AS result
           FROM _claims"#,
    )
    .bind(scope)
    .bind(format!("scope-closure-package-v2-e2e-{}", fixture.actor))
    .bind(fixture.actor.to_string())
    .fetch_one(pool)
    .await?;
    let result = row.try_get::<Value, _>("result")?;
    anyhow::ensure!(
        result["ok"] == json!(true),
        "closure request failed: {result}"
    );
    Ok((
        serde_json::from_value(result["data"]["closureCheckId"].clone())?,
        serde_json::from_value(result["data"]["workerJob"]["id"].clone())?,
    ))
}

async fn load_certificate(pool: &PgPool, check_id: Uuid) -> anyhow::Result<Certificate> {
    let row = sqlx::query(
        r#"SELECT c.requested_scope_hash,c.policy_fingerprint,c.snapshot_id,c.snapshot_hash,
             c.snapshot_artifact_id,c.snapshot_index_sha256,c.snapshot_build_contract_hash,
             c.effective_scope_hash,c.data_snapshot_token,c.closure_bundle_hash,
             c.effective_scope_manifest,a.artifact_url
           FROM public.lcia_scope_closure_checks c
           JOIN public.lca_snapshot_artifacts a ON a.id=c.snapshot_artifact_id
           WHERE c.id=$1 AND c.status='passed' AND c.certificate_status='valid'"#,
    )
    .bind(check_id)
    .fetch_one(pool)
    .await?;
    Ok(Certificate {
        check_id,
        requested_scope_hash: row.try_get("requested_scope_hash")?,
        policy_fingerprint: row.try_get("policy_fingerprint")?,
        snapshot_id: row.try_get("snapshot_id")?,
        snapshot_hash: row.try_get("snapshot_hash")?,
        snapshot_artifact_id: row.try_get("snapshot_artifact_id")?,
        snapshot_index_sha256: row.try_get("snapshot_index_sha256")?,
        snapshot_build_contract_hash: row.try_get("snapshot_build_contract_hash")?,
        effective_scope_hash: row.try_get("effective_scope_hash")?,
        data_snapshot_token: row.try_get("data_snapshot_token")?,
        closure_bundle_hash: row.try_get("closure_bundle_hash")?,
        effective_scope: row.try_get("effective_scope_manifest")?,
        artifact_url: row.try_get("artifact_url")?,
    })
}

async fn request_build(
    pool: &PgPool,
    fixture: &Fixture,
    certificate: &Certificate,
    label: &str,
) -> anyhow::Result<Build> {
    let methods = certificate.effective_scope["lciaMethods"].clone();
    let row = sqlx::query(
        r#"WITH _claims AS (
             SELECT set_config('request.jwt.claim.role','authenticated',true),
                    set_config('request.jwt.claim.sub',$7,true)
           )
           SELECT public.cmd_lcia_result_build_request_v2(
             $1,null::jsonb,'subset',null,$2,$3,$4,$5,$6,'{}'::jsonb) AS result
           FROM _claims"#,
    )
    .bind(format!("E2E {label}"))
    .bind(methods)
    .bind(format!(
        "scope-closure-package-v2-e2e-{label}-{}",
        fixture.actor
    ))
    .bind(certificate.check_id)
    .bind(&certificate.requested_scope_hash)
    .bind(&certificate.policy_fingerprint)
    .bind(fixture.actor.to_string())
    .fetch_one(pool)
    .await?;
    let result = row.try_get::<Value, _>("result")?;
    anyhow::ensure!(
        result["ok"] == json!(true),
        "build request failed: {result}"
    );
    let build_id = serde_json::from_value(result["data"]["buildId"].clone())?;
    let worker_job_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT id FROM public.worker_jobs WHERE job_kind='lcia_result.package_build' AND subject_id=$1",
    )
    .bind(build_id)
    .fetch_one(pool)
    .await?;
    Ok(Build {
        build_id,
        worker_job_id,
    })
}

async fn assert_record_result_v3_wire(pool: &PgPool, check_id: Uuid) -> anyhow::Result<()> {
    let row = sqlx::query(
        r#"SELECT p.pronargs::int AS argument_count, pg_get_functiondef(p.oid) AS definition,
             c.evidence_hash
           FROM pg_proc p
           JOIN pg_namespace n ON n.oid=p.pronamespace AND n.nspname='public'
           JOIN public.lcia_scope_closure_checks c ON c.id=$1
           WHERE p.proname='svc_lcia_scope_closure_check_record_result_v3'"#,
    )
    .bind(check_id)
    .fetch_one(pool)
    .await?;
    anyhow::ensure!(row.try_get::<i32, _>("argument_count")? == 13);
    anyhow::ensure!(
        row.try_get::<String, _>("definition")?
            .contains("lcia.scope-closure-evidence.v2"),
        "record_result_v3 no longer requires evidence v2"
    );
    anyhow::ensure!(
        row.try_get::<String, _>("evidence_hash")?.len() == 64,
        "passed closure omitted the persisted evidence hash"
    );
    Ok(())
}

async fn assert_build_binding_wire(
    state: &AppState,
    fixture: &Fixture,
    certificate: &Certificate,
) -> anyhow::Result<()> {
    let build = request_build(&state.pool, fixture, certificate, "wire-contract").await?;
    let claimed = claim_worker_jobs(
        &state.pool,
        "solver",
        "scope-closure-package-v2-e2e-wire-contract",
        1,
        300,
    )
    .await?;
    anyhow::ensure!(claimed.len() == 1 && claimed[0].id == build.worker_job_id);
    let job = &claimed[0];
    let row = sqlx::query(
        r#"WITH _service_role AS (
             SELECT set_config('request.jwt.claim.role','service_role',true)
           )
           SELECT public.svc_lcia_scope_closure_build_binding($1) AS result
           FROM _service_role"#,
    )
    .bind(job.id)
    .fetch_one(&state.pool)
    .await?;
    let result = row.try_get::<Value, _>("result")?;
    anyhow::ensure!(
        result["ok"] == json!(true),
        "binding probe failed: {result}"
    );
    let data = result["data"]
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("binding result omitted data object"))?;
    let expected_fields = BTreeSet::from([
        "certificateHash",
        "closureBundleArtifactId",
        "closureBundleHash",
        "closureCheckId",
        "coverageMode",
        "dataSnapshotToken",
        "effectiveScope",
        "effectiveScopeHash",
        "inputManifest",
        "inputManifestHash",
        "lciaMethodSet",
        "policyFingerprint",
        "requestedScopeHash",
        "snapshotArtifactId",
        "snapshotBuildContractHash",
        "snapshotHash",
        "snapshotId",
        "snapshotIndexSha256",
    ]);
    let actual_fields = data.keys().map(String::as_str).collect::<BTreeSet<_>>();
    anyhow::ensure!(actual_fields == expected_fields);
    anyhow::ensure!(!data.contains_key("reportArtifactManifestHash"));
    anyhow::ensure!(data["requestedScopeHash"] == json!(certificate.requested_scope_hash));
    anyhow::ensure!(data["policyFingerprint"] == json!(certificate.policy_fingerprint));
    anyhow::ensure!(data["effectiveScope"] == certificate.effective_scope);
    anyhow::ensure!(data["coverageMode"] == certificate.effective_scope["coverageMode"]);
    anyhow::ensure!(data["lciaMethodSet"] == certificate.effective_scope["lciaMethods"]);
    anyhow::ensure!(data["inputManifest"]["processes"] == certificate.effective_scope["processes"]);
    anyhow::ensure!(
        data["inputManifest"]["predicateVersion"]
            == certificate.effective_scope["eligibilityPredicateVersion"]
    );
    anyhow::ensure!(data["inputManifest"]["selectionMode"] == "closure_certificate");
    let authoritative_input_hash =
        sqlx::query_scalar::<_, String>("SELECT public.lcia_scope_closure_sha256($1::jsonb)")
            .bind(&data["inputManifest"])
            .fetch_one(&state.pool)
            .await?;
    anyhow::ensure!(data["inputManifestHash"] == json!(authoritative_input_hash));

    record_worker_job_result(
        &state.pool,
        job.id,
        job.lease_token,
        WorkerJobResult {
            status: "failed".to_owned(),
            result_json: None,
            result_schema_version: None,
            result_ref: None,
            diagnostics: None,
            error_code: Some("e2e_wire_probe_complete".to_owned()),
            error_message: Some("intentional terminal state after read-only wire probe".to_owned()),
            error_details: None,
            blocker_codes: Vec::new(),
            resolution_scope: None,
            retryable: Some(false),
        },
    )
    .await?;
    assert_zero_package(&state.pool, &build).await?;
    Ok(())
}

async fn assert_zero_package(pool: &PgPool, build: &Build) -> anyhow::Result<()> {
    let count = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM public.lcia_result_packages WHERE build_id=$1",
    )
    .bind(build.build_id)
    .fetch_one(pool)
    .await?;
    anyhow::ensure!(count == 0, "failed build unexpectedly published a package");
    Ok(())
}

async fn package_projection(pool: &PgPool, build: &Build) -> anyhow::Result<Value> {
    let row = sqlx::query(
        "SELECT to_jsonb(p) AS package FROM public.lcia_result_packages p WHERE build_id=$1 AND status='preview_ready'",
    )
    .bind(build.build_id)
    .fetch_one(pool)
    .await?;
    Ok(row.try_get("package")?)
}

async fn tamper_payload(
    pool: &PgPool,
    build: &Build,
    path: &[&str],
    value: Value,
) -> anyhow::Result<()> {
    sqlx::query(
        "UPDATE public.worker_jobs SET payload_json=jsonb_set(payload_json,$2,$3,true) WHERE id=$1",
    )
    .bind(build.worker_job_id)
    .bind(path)
    .bind(value)
    .execute(pool)
    .await?;
    Ok(())
}

fn object_key(artifact_url: &str, bucket: &str) -> anyhow::Result<String> {
    let marker = format!("/{bucket}/");
    artifact_url
        .split_once(marker.as_str())
        .map(|(_, key)| key.to_owned())
        .ok_or_else(|| {
            anyhow::anyhow!("artifact URL does not contain bucket {bucket}: {artifact_url}")
        })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a reset local Supabase DB + Storage; run scripts/run_scope_closure_package_v2_e2e.sh"]
async fn certified_snapshot_lifecycle_is_frozen_reusable_and_fail_closed() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_test_writer()
        .try_init();
    let state = Arc::new(AppState::new(&test_config()).await?);
    let fixture = setup_fixture(&state.pool).await?;

    let (check_id, closure_job_id) = request_closure(&state.pool, &fixture).await?;
    anyhow::ensure!(
        run_one_job(state.clone(), closure_job_id, "closure").await? == "completed",
        "scope closure Worker job did not complete"
    );
    let certificate = load_certificate(&state.pool, check_id).await?;
    assert_record_result_v3_wire(&state.pool, check_id).await?;
    let database_effective_scope_hash =
        sqlx::query_scalar::<_, String>("SELECT public.lcia_scope_closure_sha256($1::jsonb)")
            .bind(&certificate.effective_scope)
            .fetch_one(&state.pool)
            .await?;
    anyhow::ensure!(database_effective_scope_hash == certificate.effective_scope_hash);
    let compact_rust_json_hash = sha256(&serde_json::to_vec(&certificate.effective_scope)?);
    anyhow::ensure!(
        compact_rust_json_hash != database_effective_scope_hash,
        "fixture must distinguish compact Rust JSON hashing from authoritative PostgreSQL jsonb hashing"
    );

    let artifact_count = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM public.lca_snapshot_artifacts WHERE snapshot_id=$1 AND status='ready' AND artifact_format=$2",
    )
    .bind(certificate.snapshot_id)
    .bind(SNAPSHOT_ARTIFACT_FORMAT)
    .fetch_one(&state.pool)
    .await?;
    let snapshot_count = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM public.lca_network_snapshots WHERE id=$1 AND status='ready'",
    )
    .bind(certificate.snapshot_id)
    .fetch_one(&state.pool)
    .await?;
    anyhow::ensure!(snapshot_count == 1 && artifact_count == 1);
    anyhow::ensure!(SNAPSHOT_ARTIFACT_CONTENT_TYPE == "application/x-hdf5");

    let snapshot_bytes = state
        .object_store
        .download_object_url(&certificate.artifact_url)
        .await?;
    anyhow::ensure!(sha256(&snapshot_bytes) == certificate.snapshot_hash);
    let decoded = decode_snapshot_artifact(&snapshot_bytes)?;
    anyhow::ensure!(decoded.snapshot_id == certificate.snapshot_id);
    let binding = decoded
        .config
        .scope_closure_binding
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("snapshot omitted scope closure binding"))?;
    anyhow::ensure!(binding.effective_scope_hash == certificate.effective_scope_hash);
    anyhow::ensure!(binding.data_snapshot_token == certificate.data_snapshot_token);
    anyhow::ensure!(binding.closure_bundle_hash == certificate.closure_bundle_hash);
    let index_bytes = state
        .object_store
        .download_object_url(&derive_snapshot_index_url(&certificate.artifact_url))
        .await?;
    anyhow::ensure!(sha256(&index_bytes) == certificate.snapshot_index_sha256);
    let index: SnapshotIndexDocument = serde_json::from_slice(&index_bytes)?;
    anyhow::ensure!(index.snapshot_id == certificate.snapshot_id);
    let expected_axis = certificate.effective_scope["processes"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("effective scope omitted process axis"))?;
    anyhow::ensure!(index.process_map.len() == expected_axis.len());
    for (actual, expected) in index.process_map.iter().zip(expected_axis) {
        anyhow::ensure!(actual.process_id.to_string() == expected["id"]);
        anyhow::ensure!(actual.process_version == expected["version"]);
    }
    assert_build_binding_wire(&state, &fixture, &certificate).await?;

    let build_before =
        request_build(&state.pool, &fixture, &certificate, "before-mutation").await?;
    anyhow::ensure!(
        run_one_job(state.clone(), build_before.worker_job_id, "build-before").await?
            == "completed"
    );
    let before = package_projection(&state.pool, &build_before).await?;

    let mutated = process_document(
        fixture.processes[0],
        fixture.product_flows[0],
        fixture.elementary_flow,
        999.0,
        None,
    );
    sqlx::query("ALTER TABLE public.processes DISABLE TRIGGER USER")
        .execute(&state.pool)
        .await?;
    sqlx::query("UPDATE public.processes SET json=$2,json_ordered=$2::text::json WHERE id=$1")
        .bind(fixture.processes[0])
        .bind(mutated)
        .execute(&state.pool)
        .await?;
    sqlx::query("ALTER TABLE public.processes ENABLE TRIGGER USER")
        .execute(&state.pool)
        .await?;

    let build_after = request_build(&state.pool, &fixture, &certificate, "after-mutation").await?;
    anyhow::ensure!(
        run_one_job(state.clone(), build_after.worker_job_id, "build-after").await? == "completed"
    );
    let after = package_projection(&state.pool, &build_after).await?;
    for package in [&before, &after] {
        anyhow::ensure!(package["snapshot_id"] == json!(certificate.snapshot_id));
        anyhow::ensure!(
            package["artifact_manifest"]["snapshotSource"]["mode"] == "certified_snapshot_reuse_v1"
        );
        anyhow::ensure!(
            package["artifact_manifest"]["snapshotSource"]["liveSnapshotBuilderInvoked"]
                == json!(false)
        );
    }
    let before_query = state
        .object_store
        .download_object_url(
            before["query_artifact_ref"]["artifactUrl"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("before package omitted query artifact URL"))?,
        )
        .await?;
    let after_query = state
        .object_store
        .download_object_url(
            after["query_artifact_ref"]["artifactUrl"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("after package omitted query artifact URL"))?,
        )
        .await?;
    let before_query: Value = serde_json::from_slice(&before_query)?;
    let after_query: Value = serde_json::from_slice(&after_query)?;
    let expected_h = json!(vec![vec![6.0; 25], vec![11.2; 25]]);
    anyhow::ensure!(before_query["h_matrix"] == expected_h);
    anyhow::ensure!(before_query["h_matrix"] == after_query["h_matrix"]);
    anyhow::ensure!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM public.lca_network_snapshots")
            .fetch_one(&state.pool)
            .await?
            == 1
    );

    for (label, path, value) in [
        ("tampered-hash", vec!["snapshot_hash"], json!(repeated('0'))),
        (
            "tampered-id",
            vec!["snapshot_artifact_id"],
            json!(Uuid::new_v4()),
        ),
        (
            "tampered-config",
            vec!["snapshot_build_contract_hash"],
            json!(repeated('1')),
        ),
    ] {
        let build = request_build(&state.pool, &fixture, &certificate, label).await?;
        tamper_payload(&state.pool, &build, &path, value).await?;
        anyhow::ensure!(run_one_job(state.clone(), build.worker_job_id, label).await? == "failed");
        assert_zero_package(&state.pool, &build).await?;
    }

    let axis_build = request_build(&state.pool, &fixture, &certificate, "tampered-axis").await?;
    let mut reversed_axis = certificate.effective_scope["processes"]
        .as_array()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("effective process axis is not an array"))?;
    reversed_axis.reverse();
    tamper_payload(
        &state.pool,
        &axis_build,
        &["input_manifest", "processes"],
        Value::Array(reversed_axis),
    )
    .await?;
    anyhow::ensure!(
        run_one_job(state.clone(), axis_build.worker_job_id, "tampered-axis").await? == "failed"
    );
    assert_zero_package(&state.pool, &axis_build).await?;

    let bucket = required_env("S3_BUCKET");
    let snapshot_key = object_key(&certificate.artifact_url, &bucket)?;
    let mut corrupt_bytes = snapshot_bytes.clone();
    let last = corrupt_bytes
        .last_mut()
        .ok_or_else(|| anyhow::anyhow!("snapshot artifact was empty"))?;
    *last ^= 0xff;
    state
        .object_store
        .upload_object_key(&snapshot_key, SNAPSHOT_ARTIFACT_CONTENT_TYPE, corrupt_bytes)
        .await?;
    let hdf_build = request_build(&state.pool, &fixture, &certificate, "tampered-hdf").await?;
    anyhow::ensure!(
        run_one_job(state.clone(), hdf_build.worker_job_id, "tampered-hdf").await? == "failed"
    );
    assert_zero_package(&state.pool, &hdf_build).await?;
    state
        .object_store
        .upload_object_key(
            &snapshot_key,
            SNAPSHOT_ARTIFACT_CONTENT_TYPE,
            snapshot_bytes,
        )
        .await?;

    let revoked_build = request_build(&state.pool, &fixture, &certificate, "revoked").await?;
    let event = sqlx::query_scalar::<_, Value>(
        r#"WITH _claims AS (
             SELECT set_config('request.jwt.claim.role','service_role',true),
                    set_config('request.jwt.claim.sub',$2,true)
           )
           SELECT public.svc_lcia_scope_closure_certificate_event(
             $1,'revoked','e2e revocation fence') FROM _claims"#,
    )
    .bind(certificate.check_id)
    .bind(fixture.actor.to_string())
    .fetch_one(&state.pool)
    .await?;
    anyhow::ensure!(event["ok"] == json!(true));
    anyhow::ensure!(
        run_one_job(state.clone(), revoked_build.worker_job_id, "revoked").await? == "failed"
    );
    assert_zero_package(&state.pool, &revoked_build).await?;

    let ready_packages = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM public.lcia_result_packages WHERE closure_check_id=$1 AND status='preview_ready'",
    )
    .bind(certificate.check_id)
    .fetch_one(&state.pool)
    .await?;
    anyhow::ensure!(ready_packages == 2);
    anyhow::ensure!(certificate.snapshot_artifact_id != Uuid::nil());
    anyhow::ensure!(!certificate.snapshot_build_contract_hash.is_empty());
    Ok(())
}
