Davano Innovation Labs — bounded robustness checks

Synthetic endpoints only. Requires cached flowbridge-media-smoke:local and
adobe/s3mock:5.1.0 images. No account or external service is used.
From the repository root, using the desired Docker context:

  docker build --network=none --pull=false -f tests/integration/stress/Dockerfile -t flowbridge-stress:local .
  docker compose -f tests/integration/stress/compose.yaml up -d --pull never
  docker run --rm --network flowbridge-stress_default --memory 512m --cpus 1 --cap-drop ALL --security-opt no-new-privileges flowbridge-stress:local
  docker compose -f tests/integration/stress/compose.yaml down --volumes

Only use the dedicated flowbridge-stress project when it is not already in use.
Always run the cleanup command, including after a failure. Test services have no
published ports and use an internal network. Combined memory caps are 1536MiB.
The workload uploads 1101 objects in each lane, including an 11MiB binary per
lane, exercises S3 pagination, reconciles every SHA256 and media_type value,
restarts from SQLite, then injects a real TCP connection refusal and restores
access to check recovery. It tests a single polling worker with synthetic S3Mock;
it is not an Amazon S3, production throughput, or multi-day endurance benchmark.

  PYTHONPATH=. python3 tests/integration/stress/api_load.py
  node tests/ui-contract.cjs
  python3 -m unittest discover -s tests -q

The API test starts an ephemeral localhost server, sends 480 mixed requests with
16 clients, and closes it afterwards. 120 malformed flow requests must be
rejected. The UI test executes actual application JavaScript against a minimal
DOM contract: busy controls, example selection, review gate, stale artifacts,
blockers, network failure and handover guidance. It is not a visual browser,
mobile layout, keyboard/screen-reader, or human usability assessment.

Recovery regression tests also cover source changes during reads, oversized
objects, repeated pagination tokens, metadata mismatch, failed rebaseline and
an injected failure between destination write and SQLite checkpoint. The latter
simulates a crash window; it is not a hardware power-loss test.
