# Reproduces the measured matrix recorded in the 0.3 NATS report.
# Tree: 3 (publishers) x 3 (payload) x 3 (flush) x 2 (transports) x 2 (repeats)
# = 108 cells, 100k messages/cell, release build, loopback subscriber.
#
# Prereqs: spikes/nats/.target already built (the runner builds anyway), and the
# three-node Compose stack up:
#   docker compose -p nats-spike-sub -f spikes/nats/compose.yml up -d
# (the runner can manage its own project; this wrapper uses -SkipComposeUp and
# assumes the cluster is already up, to keep one cluster for all cells.)
& (Join-Path $PSScriptRoot "load-matrix.ps1") `
    -Transports tcp,wss `
    -Publishers 1,4,16 `
    -Payloads 64,256,1024 `
    -Flushes 16,64,256 `
    -Total 100000 `
    -Repeats 2 `
    -Label sweep `
    -SkipComposeUp
