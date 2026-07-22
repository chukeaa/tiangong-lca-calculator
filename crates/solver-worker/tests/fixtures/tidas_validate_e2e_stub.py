#!/usr/bin/env python3
"""Deterministic protocol seam for the local scope-closure lifecycle E2E.

This does not test TIDAS validation semantics. It implements only the validator
process protocol so the ignored test can exercise the real Worker, database,
snapshot builder, HDF5 codec, and S3-compatible object store deterministically.
"""

import hashlib
import json
import sys


PROTOCOL = "document-validation-batch.v1"
PROFILE = "tidas-document-conformance.v1"


def argument(name: str) -> str:
    index = sys.argv.index(name)
    return sys.argv[index + 1]


if "--describe" in sys.argv:
    print(
        json.dumps(
            {
                "package": {"name": "scope-closure-e2e-protocol-seam", "version": "1"},
                "protocols": [PROTOCOL],
                "engines": {"deterministicProtocolSeam": "v1"},
                "tidas_schema_lock_sha256": "0" * 64,
            },
            separators=(",", ":"),
        )
    )
    raise SystemExit(0)

manifest_path = argument("--input-manifest")
with open(manifest_path, encoding="utf-8") as manifest:
    document_count = sum(1 for line in manifest if line.strip())

print(
    json.dumps(
        {
            "type": "final",
            "schema_version": "tidas.validation-final-event.v1",
            "protocol": argument("--protocol"),
            "profile": argument("--profile"),
            "completed": True,
            "summary": {"document_count": document_count, "issue_count": 0},
            "logical_issue_stream_sha256": hashlib.sha256(b"").hexdigest(),
        },
        separators=(",", ":"),
    )
)
