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
#     Gradle would resolve the release instead of the preview, so after a release the default
#     version in the root build.gradle.kts moves first (see the header of release.yml);
#   - lists the modules `publishToMavenCentral` would upload (a dry run), in its order;
#   - uploads them one by one with `:<module>:publishToMavenCentral`, and a module once uploaded is
#     never uploaded again in this run: a rejected upload is repeated for the module it stopped at,
#     not from the first module;
#   - waits before repeating an upload the server refused for its rate (429) or for a server error
#     (5xx), longer at every refusal in a row: the error Gradle prints carries no Retry-After
#     header, and two repeats 3 and 6 minutes after a 429 were refused at their first request, so
#     the first wait is 10 minutes. Any other failure (a compile error, refused credentials) is not
#     repeated;
#   - gives up when the waits in a row run out or the run has waited its budget, names the modules
#     that kept their earlier snapshot and fails, so a red run is the signal that the preview is a
#     mix of commits until the next run completes.
#
# Settings (environment): SNAPSHOT_RETRY_DELAYS, the waits in seconds after one refusal, two in a
# row, ... (default "600 900 1200 1800"); SNAPSHOT_WAIT_BUDGET, the most seconds one run waits in
# all (default 5400). GRADLEW, GIT and SLEEP name the commands the script runs (defaults ./gradlew,
# git and sleep); its tests replace them.
set -uo pipefail

gradlew=${GRADLEW:-./gradlew}
git_cmd=${GIT:-git}
sleep_cmd=${SLEEP:-sleep}
retry_delays=${SNAPSHOT_RETRY_DELAYS:-600 900 1200 1800}
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
if ! tags=$("$git_cmd" ls-remote --tags --refs origin 'refs/tags/v*'); then
  error "Could not read the tags of origin."
  exit 1
fi
while read -r _ ref; do
  [ -n "${ref:-}" ] || continue
  tag=${ref#refs/tags/v}
  tag_base=${tag%%-*}
  [[ "$tag_base" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || continue
  # A snapshot sorts below the release of its own MAJOR.MINOR.PATCH (1.0.0-alpha-SNAPSHOT and
  # 1.0.0-SNAPSHOT before 1.0.0), and below every higher release.
  if numeric_below "$base" "$tag_base" || { [ "$base" = "$tag_base" ] && [ "$tag" = "$tag_base" ]; }; then
    error "The development version $version sorts below the release $tag (tag v$tag): Maven and" \
      "Gradle would pick the release over this preview. Move the default version in the root" \
      "build.gradle.kts above $tag first (the steps after a release are in release.yml)."
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
log=$(mktemp)
trap 'rm -f "$log"' EXIT
waited=0
published=0
for module in "${modules[@]}"; do
  refusals=0
  while true; do
    "$gradlew" "$module:publishToMavenCentral" "${gradle_flags[@]}" 2>&1 | tee "$log"
    if [ "${PIPESTATUS[0]}" -eq 0 ]; then
      published=$((published + 1))
      break
    fi
    if ! grep -qiE 'status code (429|5[0-9][0-9])|Too Many Requests' "$log"; then
      error "Publishing $module failed, and not on the snapshot repository's rate limit; not repeated."
      break 2
    fi
    if [ "$refusals" -ge "${#delays[@]}" ]; then
      error "The snapshot repository refused $module $((refusals + 1)) times in a row."
      break 2
    fi
    delay=${delays[refusals]}
    if [ $((waited + delay)) -gt "$wait_budget" ]; then
      error "The snapshot repository refused $module again, and this run has waited $waited of its $wait_budget seconds."
      break 2
    fi
    refusals=$((refusals + 1))
    echo "::warning::The snapshot repository refused $module (refusal $refusals in a row); repeating its upload in $delay seconds."
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
