# Security Policy

StreamRune handles event data, personal data (GDPR crypto-shredding) and authorization, so we take
security reports seriously.

## Reporting a vulnerability

Please do **not** open a public issue for a security problem.

- Preferred: GitHub private vulnerability reporting — the **Report a vulnerability** button on the
  repository's *Security* tab.
- Alternatively: e-mail **licensing@streamrune.com** with the subject "SECURITY".

Include the affected version or commit, a description, and steps or a test that reproduce the
problem. We acknowledge a report within 5 working days and keep you informed until it is resolved.
Please give us reasonable time to release a fix before disclosing details publicly.

## Supported versions

Only the latest release receives fixes. A fix lands on `main` first, so the preview build (built
from `main`; the [Preview builds](README.md#preview-builds) section of the README on `main` names its
version) carries it before the next release does. Move every StreamRune module of a build to the
preview version together: a module the build also reaches at a release version resolves to the
higher of the two.
