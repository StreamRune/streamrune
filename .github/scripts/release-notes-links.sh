#!/usr/bin/env bash
# Points every relative link of release notes at the file it names at the release tag, for the
# Release workflow (release.yml), and fails when a link that would not resolve is left.
#
# usage: release-notes-links.sh <notes file> <base URL>
#   <base URL> is the tree at the tag, https://github.com/<owner>/<repo>/blob/v<version>
#
# A relative link resolves in GitHub's view of CHANGELOG.md, but a release page resolves it against
# .../releases/tag/, where no file is. So every relative link target, `[text](README.md#anchor)`,
# `[text](./docs/x.md)` or a reference definition `[label]: docs/x.md`, becomes <base URL>/<target>,
# and a relative image <base URL>/<target>?raw=true. Anchors (#...), absolute URLs (https:, mailto:,
# any scheme) stay as written. A link target the rewrite cannot read (one with a space or a
# parenthesis, a site-relative /path or an <...> target) is reported, and the script exits 1, so the
# Release workflow refuses notes with a link that would not resolve.
set -euo pipefail

notes=${1:?usage: release-notes-links.sh <notes file> <base URL>}
base=${2:?usage: release-notes-links.sh <notes file> <base URL>}
base=${base%/}

# A relative target: neither an anchor, nor a site-relative path, nor a URL with a scheme.
relative='(?![#\/<]|[A-Za-z][A-Za-z0-9+.\-]*:)'
BASE="$base" RELATIVE="$relative" perl -pe '
  my $rel = $ENV{RELATIVE};
  my $base = $ENV{BASE};
  s{(!\[[^\]]*\])\((?:\./)?$rel([^\s()]+)\)}{$1($base/$2?raw=true)}g;
  s{(?<!!)(\[[^\]]*\])\((?:\./)?$rel([^\s()]+)\)}{$1($base/$2)}g;
  s{^(\s{0,3}\[[^\]]+\]:\s*)(?:\./)?$rel(\S+)}{$1$base/$2};
' "$notes" > "$notes.tmp"
mv "$notes.tmp" "$notes"

# What a release page can resolve: an anchor or an absolute URL.
left=$(perl -ne '
  print "$.: $_" if /\]\((?![#]|[A-Za-z][A-Za-z0-9+.\-]*:)/ || /^\s{0,3}\[[^\]]+\]:\s*(?![#]|[A-Za-z][A-Za-z0-9+.\-]*:)\S/;
' "$notes")
if [ -n "$left" ]; then
  echo "::error::The release notes have links a release page cannot resolve; write them absolute in CHANGELOG.md:"
  printf '%s\n' "$left"
  exit 1
fi
