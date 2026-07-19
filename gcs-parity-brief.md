# GCS Remote Bin Shipped Brief

## Objective

Google Cloud Storage remote bin support is now shipped end to end as first-class parity with AWS S3, with automatic retention and provider-aware lifecycle management. This brief replaces the prior "GCS complete except remote bin" framing and serves as the canonical handoff artifact for current status plus remaining recommended cloud validation.

## Historical Context

- GCS is shipped in-repo as a first-class provider for credentials, sync-location flows, object operations, version-aware flows, storage class changes, and remote bin flows.
- Canonical provider identity remains `gcs`; legacy `gcp` stays compatibility input only.
- Credential/storage constraints remain unchanged: AWS secrets stay in secure store/keyring; Windows GCS secrets stay as DPAPI-encrypted files referenced by credential metadata; secure-store payloads must remain read-compatible with legacy entries lacking tagged `kind`.
- The former remote-bin parity gap is closed in-repo; remaining work is operational confidence via real-cloud validation.

## Shipped Design Decisions

- Remote-bin namespace/path semantics remain pair-scoped and provider-neutral. Do not introduce GCS-specific remote-bin path rules.
- GCS remote bin ships with automatic lifecycle retention; there is no degraded or manual-cleanup mode.
- Lifecycle reconciliation must move from bucket-wide namespace rule management to bucket-aggregated planning over pair-specific prefixes.
- Lifecycle operations must move behind the provider-aware seam rather than remaining AWS-specific orchestration.
- GCS move/rewrite is hardened for multi-step rewrite-token handling before remote bin enablement.
- Frontend capability gating and UI enable remote bin for GCS through backend/provider capability support.

## Delivered Implementation Slices

1. Hardened GCS move/rewrite semantics.
   - Provider-side rewrite handles tokenized multi-step rewrites to completion for large objects and storage-class/location-driven rewrites.
   - This is the prerequisite that made GCS remote-bin restore/delete/move flows safe to enable.

2. Moved lifecycle operations behind the provider-aware seam.
   - Lifecycle planning/apply interfaces are exposed through the existing storage seam rather than AWS-specific paths.
   - AWS behavior is preserved while provider-specific lifecycle rule generation and reconciliation are supported.

3. Replaced bucket-wide namespace lifecycle reconciliation with bucket-aggregated, pair-prefix-scoped planning.
   - Desired retention state is computed per bucket across all configured pairs.
   - Lifecycle rules reconcile against pair-specific remote-bin prefixes rather than one bucket-wide namespace rule.
   - Unrelated lifecycle configuration owned outside Storage Goblin is preserved.

4. Implemented GCS lifecycle retention support for remote bin.
   - Provider-specific rule planning/apply for GCS creates and maintains remote-bin retention automatically.
   - The implementation matches the pair-scoped retention model used by remote bin semantics while respecting GCS lifecycle API constraints.

5. Enabled backend remote-bin capability for GCS.
   - Explicit GCS remote-bin guards are removed now that rewrite hardening and lifecycle reconciliation are complete.
   - Provider metadata/capabilities advertise GCS remote-bin support.

6. Enabled frontend gating and UI for GCS remote bin.
   - Remote-bin actions, messaging, and capability checks are on for GCS through the existing provider-aware frontend state/capability flow.
   - UX/provider inference rules remain unchanged.

## Validation Requirements

- Automated coverage must verify GCS rewrite completion across multi-step token flows.
- Automated coverage must verify lifecycle planning/reconciliation across multiple pairs sharing one bucket, including add/update/remove scenarios.
- Automated coverage must verify unrelated lifecycle rules are preserved during reconciliation.
- Automated coverage must verify provider capability hydration/gating enables remote bin for GCS only when backend support is active.

## Remaining Recommended Manual Cloud Validation

- Exercise end-to-end remote-bin flows on a real GCS bucket: move to bin, list, restore, permanent delete, restart/reload, and mixed multi-pair retention behavior.
- Revalidate large-object move/restore flows to confirm multi-step rewrite handling under real network and bucket conditions.
- Revalidate lifecycle reconciliation against realistic shared-bucket layouts to confirm pair-prefix aggregation and preservation of non-Goblin lifecycle rules.
- On Windows, revalidate DPAPI-backed GCS credential resolution through restart while remote-bin flows are enabled.

## Risks And Manual Validation Focus

- GCS rewrite behavior is the main functional risk; incomplete token-loop handling can break large-object move/restore flows.
- GCS lifecycle APIs may impose rule-shape limits or matching constraints that complicate pair-prefix aggregation; validate against realistic multi-pair bucket layouts.
- Lifecycle reconciliation must not regress AWS behavior or remove non-Goblin lifecycle rules.
- Frontend enablement before backend readiness would expose broken flows; keep gating strictly capability-driven.
- On Windows, revalidate DPAPI-backed GCS credential resolution through restart while remote-bin flows are enabled.

## Current Shipped State

- GCS remote bin is enabled end to end with automatic retention.
- Lifecycle reconciliation is provider-aware and bucket-aggregated by pair prefix.
- GCS rewrite/move is proven reliable for tokenized multi-step rewrites.
- Backend capabilities and frontend gating both advertise GCS remote bin as supported.
- Additional real-bucket manual validation remains recommended on supported desktop targets.
