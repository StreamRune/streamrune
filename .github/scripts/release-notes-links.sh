#!/usr/bin/env bash
# Points every relative link of release notes at the file it names at the release tag, for the
# Release workflow (release.yml), and fails when a link that would not resolve is left.
#
# usage: release-notes-links.sh <notes file> <base URL>
#   <base URL> is the tree at the tag, https://github.com/<owner>/<repo>/blob/v<version>
#
# A relative link resolves in GitHub's view of CHANGELOG.md, but a release page resolves it against
# .../releases/tag/, where no file is. So the target of every relative link becomes
# <base URL>/<target>: an inline link `[text](README.md#anchor)` or `[text](./docs/x.md "Title")`, a
# reference definition `[label]: docs/x.md`, and an image, whose target gets `?raw=true` (or
# `&raw=true` after a query of its own). Anchors (#...), absolute URLs (https:, mailto:, any scheme)
# and footnote definitions (`[^1]: ...`) stay as written. Links inside code spans are rewritten
# like any other. What the rewrite cannot point at the tag is reported, and the script exits 1, so
# the Release workflow refuses the notes: a target with a space or a parenthesis, a site-relative
# /path, an <...> target, a target that leaves the tree (../), and a relative href="..." or src="..."
# of an HTML tag.
set -euo pipefail

notes=${1:?usage: release-notes-links.sh <notes file> <base URL>}
base=${2:?usage: release-notes-links.sh <notes file> <base URL>}
base=${base%/}

# A relative target: neither an anchor, nor a site-relative path, nor one that leaves the tree, nor
# a URL with a scheme.
relative='(?![#\/<]|\.\.\/|[A-Za-z][A-Za-z0-9+.\-]*:)'
BASE="$base" RELATIVE="$relative" perl -pe '
  my $rel = $ENV{RELATIVE};
  my $base = $ENV{BASE};
  my $title = qr{(\s+"[^"]*")?};
  s{(!\[[^\]]*\])\((?:\./)?$rel([^\s()?]+)\?([^\s()]*)$title\)}{$1($base/$2?$3&raw=true$4)}g;
  s{(!\[[^\]]*\])\((?:\./)?$rel([^\s()?]+)$title\)}{$1($base/$2?raw=true$3)}g;
  s{(?<!!)(\[[^\]]*\])\((?:\./)?$rel([^\s()]+)$title\)}{$1($base/$2$3)}g;
  s{^(\s{0,3}\[(?!\^)[^\]]+\]:\s*)(?:\./)?$rel(\S+)}{$1$base/$2};
' "$notes" > "$notes.tmp"
mv "$notes.tmp" "$notes"

# What a release page can resolve: an anchor or an absolute URL.
left=$(perl -ne '
  my $abs = qr{(?:[#]|[A-Za-z][A-Za-z0-9+.\-]*:)};
  print "$.: $_" if /\]\((?!$abs)/
    || /^\s{0,3}\[(?!\^)[^\]]+\]:\s*(?!$abs)\S/
    || /\b(?:href|src)\s*=\s*["\x27](?!$abs)/;
' "$notes")
if [ -n "$left" ]; then
  echo "::error::The release notes have links a release page cannot resolve; write them absolute in CHANGELOG.md:"
  printf '%s\n' "$left"
  exit 1
fi
