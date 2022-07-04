import org.kohsuke.github.GitHub;
import org.kohsuke.github.GitHubBuilder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

///usr/bin/env jbang "$0" "$@" ; exit $?
//DEPS org.kohsuke:github-api:1.307
public class UpdateDocsAttributesFiles {

    public static void main(String... args) throws IOException {
        var gh = new GitHubBuilder().build();
        var lastMutinyRelease = gh.getRepository("smallrye/smallrye-mutiny").getLatestRelease().getName();
        var lastBindingsRelease = gh.getRepository("smallrye/smallrye-mutiny-vertx-bindings").getLatestRelease().getName();

        // Use multiline blocks when Java 17 will be the baseline
        var indent = "    ";
        var newline = "\n";
        var builder = new StringBuilder();
        builder
                .append("attributes:").append(newline)
                .append(indent).append("project-version: ").append(lastMutinyRelease).append(newline)
                .append(indent).append("versions:").append(newline)
                .append(indent).append(indent).append("mutiny: ").append(lastMutinyRelease).append(newline)
                .append(indent).append(indent).append("vertx_bindings: ").append(lastBindingsRelease).append(newline);

        Files.writeString(Path.of("docs/attributes.yaml"), builder.toString(), StandardCharsets.UTF_8);
    }
}
