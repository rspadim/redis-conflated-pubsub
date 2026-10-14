# Code signing

Status: **inactive**. The Windows binary shipped in `windows-x86_64.zip` is **not
signed** (`Get-AuthenticodeSignature` reports `NotSigned`), and nothing in the CI
signs it. When a token exists it will be applied **manually** first (outside the
pipeline), and only wired into the CI after that manual flow is proven.

## What gets signed (when a token exists)

| Artifact | Why |
| --- | --- |
| `redis-conflated-pubsub.exe` (inside `windows-x86_64.zip`) | the file a Windows user downloads and runs; an unsigned PE is a classic SmartScreen/AV false positive |

Authenticode covers Windows PE files only: the Linux and macOS archives carry the
same program for those platforms and are out of scope here (their `.sha256` files
are the integrity check).

In `release.yml` the signature belongs **inside the Windows job, between the cargo
build and the "Package Windows binary" step**: the zip and its `.sha256` are
produced by that step, so signing after packaging would publish a hash that
describes the unsigned file.

## Manual procedure (the current plan)

1. Build the release binary (`cargo build --release --locked --target
   x86_64-pc-windows-msvc`) or download the `windows-x86_64` artifact from a CI run.
2. Sign the exe and verify it (see below).
3. Re-package `windows-x86_64.zip`, regenerate `windows-x86_64.zip.sha256`, and only
   then attach both to the release.

## Service

Free Windows code signing for open-source projects is being pursued through two
programs; neither is active for this project yet.

- **Necessary Code Signing** (<https://sign.necessary.nu/>) - the certificate is
  issued to *Necessary Innovations AB (Sweden)*, so a signed binary shows that
  publisher; identity and address are verified before a token is granted, and the
  service signs established projects. The current application covers two sibling
  Excel add-ins (RedisExcel and NATSExcel), so this repository has to be added to it
  before it can be signed. Signing is three `osslsigncode` steps, which is what makes
  it usable from CI:

  ```
  osslsigncode extract-data -in redis-conflated-pubsub.exe -out tosign.bin
  curl -X POST -H "Authorization: Bearer $SIGNING_TOKEN" \
    --data-binary @tosign.bin https://sign.necessary.nu/windows/sign -o signed.bin
  osslsigncode attach-signature -sigin signed.bin -in redis-conflated-pubsub.exe \
    -out redis-conflated-pubsub-signed.exe
  ```

- **SignPath Foundation** (<https://signpath.org/>) - the other free lane; the
  certificate is issued to the foundation, and it additionally requires a published
  code-signing policy on the project's home page plus a manually approved signing
  request per release.

### When a token exists

1. Sign manually first, then re-package and re-hash.
2. Verify (`Get-AuthenticodeSignature`, or `osslsigncode verify` on what the service
   returns): the status must be `Valid` and the publisher must be the signing
   service's, not a local certificate.
3. Only then wire it into the CI as a guarded step - a fork or a PR has no token and
   must not fail - and keep the post-signing verification as the gate: a
   misconfigured signing step must fail the job, never ship a half-signed release.

## Never

No script in this repository disables or reconfigures antivirus or Defender
(that is MITRE T1562.001 and any EDR will - correctly - block it). Test tooling
may only **read** the AV state.
