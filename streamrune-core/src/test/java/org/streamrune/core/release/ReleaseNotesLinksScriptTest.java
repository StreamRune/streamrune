package org.streamrune.core.release;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@code .github/scripts/release-notes-links.sh}, which the Release workflow runs over the release
 * notes: a relative link resolves in GitHub's view of {@code CHANGELOG.md} but not on a release
 * page, so every one must leave the script pointing at its file at the tag, and a link it cannot
 * rewrite must refuse the release.
 */
class ReleaseNotesLinksScriptTest {

  private static final String SCRIPT = ".github/scripts/release-notes-links.sh";

  private static final String BASE = "https://github.com/StreamRune/streamrune/blob/v1.0.0";

  @TempDir Path dir;

  @Test
  void aRelativeLinkOutsideDocsPointsAtTheFileAtTheTag() throws IOException {
    String notes = rewrite("See [API stability](README.md#api-stability).\n", 0);

    assertThat(notes).isEqualTo("See [API stability](" + BASE + "/README.md#api-stability).\n");
  }

  @Test
  void linksIntoDocsImagesAndReferenceDefinitionsPointAtTheTagToo() throws IOException {
    String notes =
        rewrite(
            """
            [page](docs/reference/release-1.0.0.md) and [guide](./docs/guide/production.md#jvms)
            ![chart](docs/chart.png)
            [contributing]: CONTRIBUTING.md
            """,
            0);

    assertThat(notes)
        .isEqualTo(
            "[page]("
                + BASE
                + "/docs/reference/release-1.0.0.md) and [guide]("
                + BASE
                + "/docs/guide/production.md#jvms)\n![chart]("
                + BASE
                + "/docs/chart.png?raw=true)\n[contributing]: "
                + BASE
                + "/CONTRIBUTING.md\n");
  }

  @Test
  void aTitledLinkAndAnImageWithAQueryPointAtTheTagToo() throws IOException {
    String notes = rewrite("[g](docs/g.md \"The guide\") ![c](docs/chart.svg?sanitize=true)\n", 0);

    assertThat(notes)
        .isEqualTo(
            "[g]("
                + BASE
                + "/docs/g.md \"The guide\") ![c]("
                + BASE
                + "/docs/chart.svg?sanitize=true&raw=true)\n");
  }

  @Test
  void aFootnoteDefinitionIsNotALink() throws IOException {
    String text = "A claim.[^1]\n\n[^1]: Measured on the nightly runs.\n";

    assertThat(rewrite(text, 0)).isEqualTo(text);
  }

  @Test
  void aLinkOutOfTheTreeOrARelativeHtmlLinkRefusesTheNotes() throws IOException {
    assertThat(run("[other](../other/README.md)\n").exitCode()).isEqualTo(1);
    assertThat(run("<a href=\"docs/x.md\">x</a>\n").output())
        .contains("cannot resolve")
        .contains("1: <a href=");
    assertThat(run("<img src='docs/a.png'>\n").exitCode()).isEqualTo(1);
    assertThat(run("<a href=\"https://example.com\">x</a>\n").exitCode()).isZero();
  }

  @Test
  void anchorsAndAbsoluteLinksStayAsWritten() throws IOException {
    String text =
        """
        [here](#api-stability), [semver](https://semver.org/spec/v2.0.0.html), \
        [mail](mailto:security@example.com)
        [site]: https://example.com/x
        """;

    assertThat(rewrite(text, 0)).isEqualTo(text);
  }

  @Test
  void aLinkTheRewriteCannotReadRefusesTheNotes() throws IOException {
    assertThat(run("[root](/StreamRune/streamrune)\n").output())
        .contains("cannot resolve")
        .contains("1: [root](/StreamRune/streamrune)");
    assertThat(run("[spaced](<docs/a b.md>)\n").exitCode()).isEqualTo(1);
  }

  @Test
  void everyLinkOfTheChangelogResolvesOnAReleasePage() throws IOException {
    RepositoryScripts.Result result = run(RepositoryScripts.read("CHANGELOG.md"));

    assertThat(result.exitCode()).as(result.output()).isZero();
    assertThat(Files.readString(dir.resolve("notes.md"), StandardCharsets.UTF_8))
        .contains("[API stability](" + BASE + "/README.md#api-stability)")
        .doesNotContain("](README.md")
        .doesNotContain("](docs/");
  }

  private String rewrite(String text, int expectedExit) throws IOException {
    RepositoryScripts.Result result = run(text);
    assertThat(result.exitCode()).as(result.output()).isEqualTo(expectedExit);
    return Files.readString(dir.resolve("notes.md"), StandardCharsets.UTF_8);
  }

  private RepositoryScripts.Result run(String text) throws IOException {
    Path notes = dir.resolve("notes.md");
    Files.writeString(notes, text, StandardCharsets.UTF_8);
    return RepositoryScripts.runScript(SCRIPT, dir, Map.of(), notes.toString(), BASE);
  }
}
