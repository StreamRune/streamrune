#!/usr/bin/env bash
# Publishes the development version of every published module to the Central Portal snapshot
# repository, one module at a time, for the Publish Snapshot workflow (publish-snapshot.yml).
#
# A Maven snapshot is not one deployment: each module has its own maven-metadata.xml, written when
# that module's upload ends, so an upload that stops part-way leaves the modules before the stop on
# this commit and the modules after it on whatever commit last reached them. The snapshot
# repository rate-limits uploads (HTTP 429), so this script:
#
#   - refuses a development version that sorts below a release already tagged on origin: Maven and
#     Gradle would resolve the release instead of the preview, so the default version in the root
#     build.gradle.kts moves above a version in the pull request that prepares its release (see the
#     header of release.yml). A pre-release tag of the same MAJOR.MINOR.PATCH counts too: after
#     v1.0.0-rc.1, 1.0.0-alpha-SNAPSHOT sorts below it, 1.0.0-SNAPSHOT and 1.0.0-rc.2-SNAPSHOT above;
#   - lists the modules `publishToMavenCentral` would upload (a dry run), in its order;
#   - uploads them one by one with `:<module>:publishToMavenCentral`, and a module once uploaded is
#     never uploaded again in this run: a rejected upload is repeated for the module it stopped at,
#     not from the first module;
#   - waits before repeating an upload the server refused for its rate (429) or for a server error
#     (5xx), longer at every refusal in a row: the error Gradle prints carries no Retry-After
#     header, and two repeats 3 and 6 minutes after a 429 were refused at their first request, so
#     the first wait is 10 minutes;
#   - repeats an upload the network broke (a timeout, a reset connection, a failed TLS handshake, a
#     host that did not resolve) after a short wait, a few times in a row. Any other failure (a
#     compile error, refused credentials) is not repeated. Reading origin's tags is tried three
#     times;
#   - gives up when the waits in a row run out or the run has waited its budget, names the modules
#     that kept their earlier snapshot and fails, so a red run is the signal that the preview is a
#     mix of commits until the next run completes.
#
# Settings (environment): SNAPSHOT_RETRY_DELAYS, the waits in seconds after one refusal, two in a
# row, ... (default "600 900 1200 1800"); SNAPSHOT_NETWORK_DELAYS, the same after a network failure
# (default "90 180"); SNAPSHOT_WAIT_BUDGET, the most seconds one run waits in all (default 5400).
# GRADLEW, GIT and SLEEP name the commands the script runs (defaults ./gradlew, git and sleep); its
# tests replace them.
set -uo pipefail

gradlew=${GRADLEW:-./gradlew}
git_cmd=${GIT:-git}
sleep_cmd=${SLEEP:-sleep}
retry_delays=${SNAPSHOT_RETRY_DELAYS:-600 900 1200 1800}
network_delays=${SNAPSHOT_NETWORK_DELAYS:-90 180}
wait_budget=${SNAPSHOT_WAIT_BUDGET:-5400}
gradle_flags=(--no-parallel --max-workers=1 --console=plain)

error() { echo "::error::$*"; }
summary() { [ -n "${GITHUB_STEP_SUMMARY:-}" ] && echo "$*" >> "$GITHUB_STEP_SUMMARY"; return 0; }

# 0 when the dotted numeric version $1 sorts below $2 (1.0.0 < 1.0.1 < 1.1.0 < 2.0.0).
numeric_below() {
  local IFS=.
  local -a a b
  read -r -a a <<< "$1"
  read -r -a b <<< "$2"
  local i x y
  for i in 0 1 2; do
    x=${a[i]:-0}
    y=${b[i]:-0}
    if [ "$x" -lt "$y" ]; then return 0; fi
    if [ "$x" -gt "$y" ]; then return 1; fi
  done
  return 1
}

# The rank of a pre-release qualifier in Maven's and Gradle's order (alpha < beta < milestone < rc),
# by its leading letters; empty for one neither orders by name.
qualifier_rank() {
  case "$(printf '%s' "$1" | sed -E 's/^([A-Za-z]*).*/\1/' | tr '[:upper:]' '[:lower:]')" in
    alpha | a) echo 1 ;;
    beta | b) echo 2 ;;
    milestone | m) echo 3 ;;
    rc | cr) echo 4 ;;
    *) echo "" ;;
  esac
}

# The number in a qualifier (rc.2, rc2, alpha-1), 0 without one.
qualifier_number() {
  local n
  n=$(printf '%s' "$1" | sed -E 's/^[A-Za-z]*[.-]?([0-9]*).*/\1/')
  echo "${n:-0}"
}

# 0 when the development version (base $1, qualifier $2 between the base and -SNAPSHOT, empty for
# X.Y.Z-SNAPSHOT) sorts below the tagged version (base $3, pre-release part $4, empty for a release).
snapshot_below() {
  local base=$1 qualifier=$2 tag_base=$3 tag_pre=$4
  if numeric_below "$base" "$tag_base"; then return 0; fi
  [ "$base" = "$tag_base" ] || return 1
  [ -n "$tag_pre" ] || return 0            # X.Y.Z-anything-SNAPSHOT < X.Y.Z
  [ -n "$qualifier" ] || return 1          # X.Y.Z-SNAPSHOT > X.Y.Z-rc.1
  local mine theirs
  mine=$(qualifier_rank "$qualifier")
  theirs=$(qualifier_rank "$tag_pre")
  if [ -z "$mine" ] || [ -z "$theirs" ]; then
    echo "::warning::Cannot order $base-$qualifier-SNAPSHOT against the tag v$tag_base-$tag_pre; not checked."
    return 1
  fi
  if [ "$mine" -lt "$theirs" ]; then return 0; fi
  if [ "$mine" -gt "$theirs" ]; then return 1; fi
  [ "$(qualifier_number "$qualifier")" -le "$(qualifier_number "$tag_pre")" ]
}

commit=$("$git_cmd" rev-parse HEAD 2> /dev/null) || commit="HEAD"

# ── The development version must sort above every release tagged on origin ──────────────────
version=$("$gradlew" -q :streamrune-core:properties --console=plain | sed -n 's/^version: //p')
if [ -z "$version" ]; then
  error "Could not read the development version (./gradlew :streamrune-core:properties)."
  exit 1
fi
case "$version" in
  *-SNAPSHOT) ;;
  *) error "'$version' is not a -SNAPSHOT version; only the Release workflow publishes a release."; exit 1 ;;
esac
base=${version%%-*}
if ! [[ "$base" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  error "The development version '$version' does not start with MAJOR.MINOR.PATCH."
  exit 1
fi
qualifier=${version#"$base"}
qualifier=${qualifier%-SNAPSHOT}
qualifier=${qualifier#-}
tags=""
for attempt in 1 2 3; do
  if tags=$("$git_cmd" ls-remote --tags --refs origin 'refs/tags/v*'); then
    break
  fi
  if [ "$attempt" -eq 3 ]; then
    error "Could not read the tags of origin (three attempts)."
    exit 1
  fi
  echo "::warning::Could not read the tags of origin; trying again in 30 seconds."
  "$sleep_cmd" 30
done
while read -r _ ref; do
  [ -n "${ref:-}" ] || continue
  tag=${ref#refs/tags/v}
  tag_base=${tag%%-*}
  [[ "$tag_base" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || continue
  tag_pre=${tag#"$tag_base"}
  tag_pre=${tag_pre#-}
  # A snapshot sorts below the release of its own MAJOR.MINOR.PATCH (1.0.0-alpha-SNAPSHOT and
  # 1.0.0-SNAPSHOT before 1.0.0), below every higher version, and below a pre-release of its own
  # MAJOR.MINOR.PATCH that its qualifier does not pass.
  if snapshot_below "$base" "$qualifier" "$tag_base" "$tag_pre"; then
    error "The development version $version sorts below $tag (tag v$tag): Maven and Gradle would" \
      "pick that version over this preview. Move the default version in the root build.gradle.kts" \
      "above $tag (the steps of a release are in the header of release.yml)."
    exit 1
  fi
done <<< "$tags"

# ── The modules publishToMavenCentral uploads, in its order ───────────────────────────────────
if ! plan=$("$gradlew" publishToMavenCentral --dry-run "${gradle_flags[@]}"); then
  error "Could not list the modules to publish (./gradlew publishToMavenCentral --dry-run)."
  exit 1
fi
modules=()
while read -r task state; do
  case "$task" in
    :*:publishToMavenCentral) [ "$state" = SKIPPED ] && modules+=("${task%:publishToMavenCentral}") ;;
  esac
done <<< "$plan"
if [ "${#modules[@]}" -eq 0 ]; then
  error "The dry run of publishToMavenCentral lists no module to publish."
  exit 1
fi
echo "Publishing $version from $commit: ${#modules[@]} modules, one at a time."

# ── Upload, resuming at the module a refusal stopped ──────────────────────────────────────────
read -r -a delays <<< "$retry_delays"
read -r -a net_delays <<< "$network_delays"
log=$(mktemp)
trap 'rm -f "$log"' EXIT
waited=0
published=0
for module in "${modules[@]}"; do
  refusals=0
  breaks=0
  while true; do
    "$gradlew" "$module:publishToMavenCentral" "${gradle_flags[@]}" 2>&1 | tee "$log"
    if [ "${PIPESTATUS[0]}" -eq 0 ]; then
      published=$((published + 1))
      break
    fi
    if grep -qiE 'status code (429|5[0-9][0-9])|Too Many Requests' "$log"; then
      if [ "$refusals" -ge "${#delays[@]}" ]; then
        error "The snapshot repository refused $module $((refusals + 1)) times in a row."
        break 2
      fi
      delay=${delays[refusals]}
      refusals=$((refusals + 1))
      what="The snapshot repository refused $module (refusal $refusals in a row)"
    elif grep -qiE 'SocketTimeoutException|Read timed out|Connect timed out|Connection reset|Remote host terminated the handshake|SSLHandshakeException|UnknownHostException|NoHttpResponseException' "$log"; then
      if [ "$breaks" -ge "${#net_delays[@]}" ]; then
        error "The network broke the upload of $module $((breaks + 1)) times in a row."
        break 2
      fi
      delay=${net_delays[breaks]}
      breaks=$((breaks + 1))
      what="The network broke the upload of $module (failure $breaks in a row)"
    else
      error "Publishing $module failed, neither on the snapshot repository's rate limit nor on the network; not repeated."
      break 2
    fi
    if [ $((waited + delay)) -gt "$wait_budget" ]; then
      error "$what, and this run has waited $waited of its $wait_budget seconds."
      break 2
    fi
    echo "::warning::$what; repeating its upload in $delay seconds."
    "$sleep_cmd" "$delay"
    waited=$((waited + delay))
  done
done

if [ "$published" -eq "${#modules[@]}" ]; then
  echo "Published all ${#modules[@]} modules of $version from $commit."
  summary "Published all ${#modules[@]} modules of $version from $commit."
  exit 0
fi
stale=("${modules[@]:published}")
error "Published $published of ${#modules[@]} modules from $commit. These keep the snapshot of an" \
  "earlier commit until a later run completes: ${stale[*]}"
summary "Published $published of ${#modules[@]} modules of $version from $commit; not published: ${stale[*]}"
exit 1
