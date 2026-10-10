# Contributing to StreamRune

## Contributions — please read first

StreamRune is dual-licensed (Business Source License 1.1 and a commercial license). Until a
Contributor License Agreement (CLA) is in place, **we do not accept external code contributions**
(pull requests with code). Bug reports, reproducers, questions and feature ideas are very welcome —
please open an issue. Security problems: see `SECURITY.md`.

## Licensing of Contributions

By contributing to StreamRune, you agree that your contributions are
licensed under the Business Source License 1.1 (see `LICENSE`).

## Developer Certificate of Origin (DCO)

All commits must be signed off using the Developer Certificate of
Origin. The DCO is a lightweight legal statement: by signing off, you
certify that you wrote the code or otherwise have the right to submit
it under the project's license.

Full DCO text: <https://developercertificate.org/>

### How to sign off

Add a `Signed-off-by` trailer to every commit:

    git commit --signoff

This appends `Signed-off-by: Your Name <your-email@example.com>` using
the name and email from `git config user.name` / `user.email`. The `DCO`
check on a pull request requires that trailer to carry the name and
e-mail of the commit's author, so sign off as the author of the commit.

To retroactively sign off the most recent commit:

    git commit --amend --signoff --no-edit

## Commit messages

We use [Conventional Commits](https://www.conventionalcommits.org/): `<type>(<scope>): <summary>`,
for example `fix(outbox): release a claim the poller no longer holds`. Types: `feat`, `fix`, `docs`,
`test`, `refactor`, `perf`, `build`, `ci`, `chore`. Run `git config commit.template .gitmessage`
once to get the template in your editor.

### Pre-commit hook

StreamRune installs a pre-commit hook (via `./gradlew installGitHooks`,
run automatically on `./gradlew build`) that runs `spotlessCheck`
before every commit. If it fails, run:

    ./gradlew spotlessApply
    git add -u
    git commit --signoff

### PostgreSQL versions

StreamRune requires PostgreSQL 17 or newer, and CI runs every PostgreSQL-backed test suite of
`postgresTest` on 18 (the default) and on 17 (the `@Tag("broker")` suite runs on 18 only). To run those suites against another server version locally (Docker
required):

    ./gradlew postgresTest -PpostgresImage=postgres:17-alpine

A test never names a PostgreSQL image itself: it starts
`new PostgreSQLContainer<>(PostgresTestImage.NAME)`, which reads the image from that property
(default `postgres:18-alpine`). `./gradlew verifyPostgresTestImage` (part of `check`) fails the build
on a hard-coded image, or on a module outside the PostgreSQL-backed list in the root
`build.gradle.kts` (`postgresBackedModules`) that starts a PostgreSQL container — add the module
there, and `.github/workflows/build.yml`'s `postgres-17` job picks it up through `postgresTest`.
