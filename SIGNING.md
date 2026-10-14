# Code signing

Two files get a signature, **in this order**, and the order is not cosmetic:

1. `redis-conflated-pubsub.exe` — the relay itself, the "binary that runs".
2. `ConflatedPubSubSetup-<version>.exe` — the Inno Setup installer.

The installer **embeds** the executable and compresses it, so an executable
signed after packaging cannot be signed at all: the certificate has to be stamped
on the file before Inno packs it. Signing only the installer would leave an
unsigned executable inside a signed container, which is exactly the shape we are
trying to avoid (and, once inside the archive, it can never be fixed).

The service is provided by the [SignPath Foundation](https://signpath.org/):
free code signing for open-source projects, with the certificate issued in the
foundation's name. Application: <https://signpath.org/apply>. Documentation:
<https://docs.signpath.io/>.

## Why both

| Artifact | What breaks without a signature |
| --- | --- |
| Installer | It is the first file a user runs, so it is the #1 SmartScreen and antivirus false-positive target. |
| Executable | It is the file the Windows service loads and the file a user gets by unpacking the release zip directly. SignPath's policy also expects signed program files inside signed packages. |

Linux and macOS get nothing from this: Authenticode is a Windows mechanism, and
there is no free equivalent (Linux package signatures live in the distribution
repositories, and macOS needs an Apple Developer ID plus notarization, which is a
paid program). Only the `windows-x86_64` release asset is affected.

## Requirements this repository has to satisfy

SignPath Foundation's terms require more than a signature request:

- **Metadata on every signed binary.** "All signed binaries must have metadata
  attributes set", "Set all product name attributes to your project's name", "Set
  all product version attributes to the same value in each build". The executable
  carries `ProductName=Conflated Pub/Sub` and `FileVersion=<Cargo version>` from
  [`build.rs`](https://github.com/rspadim/redis-conflated-pubsub/blob/main/build.rs);
  the installer carries the same values from the `VersionInfo*` directives in
  [`installer/redis-conflated-pubsub.iss`](https://github.com/rspadim/redis-conflated-pubsub/blob/main/installer/redis-conflated-pubsub.iss).
- **A download page that describes the project and mentions the programme.** The
  credit line below lives in the root `README.md` and on the home page of the
  project site in all three languages.
- **An OSI-approved license with no commercial dual-licensing** and no
  proprietary components. This project is MIT throughout.
- **A verifiable reputation.** SignPath vouches for the link between the
  repository and the signed binary, so executable projects need to look real.
  This is the soft requirement, and the one a young project should expect to be
  asked about.

Attribution, required wording:

> Free code signing provided by [SignPath.io](https://signpath.io/), certificate by [SignPath Foundation](https://signpath.org/).

## Pipeline

`.github/workflows/release.yml` runs, for a `v*` tag:

1. `build` — the existing matrix. `windows-x86_64` holds the unsigned
   executable and its `.sha256`.
2. `sign-windows-exe` — unpacks the executable, submits it as a GitHub artifact
   to SignPath, waits, and re-uploads `windows-x86_64` with the **signed**
   executable and a **recomputed** `.sha256`. The checksum must cover the signed
   file, never the unsigned one.
3. `build-installer` — compiles the installer with Inno Setup, using the signed
   executable from the artifact above, then signs the installer and adds its
   `.sha256`.
4. `publish` — attaches both signed files to the release.

Everything that talks to SignPath is gated on the repository variable
`SIGNPATH_ENABLED == 'true'`. Until the certificate exists, the release keeps
producing the same unsigned assets as before.

### One-time setup

Create these in the repository (Settings → Secrets and variables → Actions):

| Kind | Name | Value |
| --- | --- | --- |
| Secret | `SIGNPATH_API_TOKEN` | API token from signpath.io, scoped to this project. |
| Variable | `SIGNPATH_ENABLED` | `true` once the certificate is approved. |
| Variable | `SIGNPATH_ORGANIZATION_ID` | Organization ID shown in signpath.io. |
| Variable | `SIGNPATH_PROJECT_SLUG` | Project slug created for this repository. |
| Variable | `SIGNPATH_SIGNING_POLICY_SLUG` | Signing policy slug (for example `release-signing`). |
| Variable | `SIGNPATH_ARTIFACT_CONFIG_EXE` | Artifact configuration slug for the executable. |
| Variable | `SIGNPATH_ARTIFACT_CONFIG_INSTALLER` | Artifact configuration slug for the installer. |

The two artifact configurations can be identical if one configuration already
matches a single `.exe`; separate slugs make it possible to describe the installer
differently later without touching the workflow.

### Checking a signature

After a release, on Windows:

```powershell
Get-AuthenticodeSignature .\ConflatedPubSubSetup-0.3.0.exe | Format-List Status, SignerCertificate
```

`Status` should be `Valid` and the signer should be the SignPath Foundation, not
the maintainer.

## Adding another signed artifact

Anything else that ships as a Windows executable (a future tray tool, a helper
binary) follows the same three-step shape: sign it **before** anything packages
it, recompute the checksum after signing, and keep the signing request gated on
the same variable. A file that only exists inside an archive cannot be signed
later.
