# ADR-003: Business Source License 1.1

**Status:** Accepted
**Date:** 2026-04-15

## Context

Licensing a framework involves a fundamental tension: open enough to attract contributors and build ecosystem trust, restrictive enough to sustain commercial development. The main candidates considered:

1. **MIT / Apache 2.0** — maximally permissive. Anyone may use, embed, redistribute, and sell products built with the framework, including competitors who embed it in a SaaS offering without contributing back. Zero friction for adoption. Zero revenue protection for the maintainer.
2. **AGPL-3.0** — copyleft triggered by network use. Any SaaS application using an AGPL library must open-source the entire application. Highly protective but widely rejected by enterprises; many legal teams prohibit AGPL dependencies outright.
3. **SSPL (Server Side Public License)** — stricter than AGPL: requires open-sourcing the entire management layer of any service that offers the software as a service. Rejected by OSI. Even higher enterprise friction than AGPL.
4. **Business Source License 1.1 (BSL 1.1)** — a source-available license with a time-delayed open source conversion. Created by MariaDB Corporation. The licensor sets an "Additional Use Grant" (revenue threshold or other condition), a "Change Date" (at which the code converts to an open source license), and a "Change License" (the post-conversion license).

StreamRune's maintainer is an individual (Martin Bednář). The framework needs to be freely usable by the open source community and by small businesses while protecting against large-scale commercial exploitation without contribution.

## Decision

Adopt BSL 1.1 with the following parameters (as specified in `LICENSE`):

- **Licensor:** Martin Bednář
- **Additional Use Grant:** Free for production use by any organization whose total annual gross revenue (combined with affiliates) does not exceed USD 5,000,000.
- **Change Date:** Four years from the date each version is first published under this license.
- **Change License:** Apache License, Version 2.0.

This means: StreamRune is free for open source projects, startups, and companies under the USD 5M revenue threshold. Companies above the threshold must obtain a commercial license from `licensing@streamrune.com`. Four years after each release, that release's source code automatically becomes Apache 2.0.

The source code is fully visible to all — users can read, audit, and contribute. This is not "closed source"; it is "source available."

## Consequences

**Positive:**

- **Free for the vast majority of users.** Most open source projects and small businesses fall well under the USD 5M revenue threshold.
- **Ecosystem trust through source availability.** Developers can read the implementation, file pull requests, and audit security properties — unlike a pure proprietary binary SDK.
- **Guaranteed open source future.** Every version converts to Apache 2.0 after four years, meaning the community can always fork and maintain any version that a commercial licensor abandons.
- **Sustainable commercial model.** Large enterprises that derive significant revenue from StreamRune are expected to contribute via a commercial license, funding continued development.

**Negative:**

- **Not OSI-approved open source.** Some enterprise legal teams and some open source foundations will refuse BSL dependencies on principle, regardless of the threshold.
- **Revenue tracking ambiguity.** The USD 5M threshold requires good-faith self-assessment. "Affiliates" are defined in the license, but edge cases (subsidiaries, joint ventures) require legal judgment.
- **Contributor license agreement complexity.** Contributions are effectively contributed under BSL 1.1 terms. Contributors should understand that their patches are not immediately Apache 2.0. A CLA may be needed for clarity.
