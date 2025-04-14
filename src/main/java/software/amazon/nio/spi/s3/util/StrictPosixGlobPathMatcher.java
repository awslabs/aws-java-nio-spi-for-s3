/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3.util;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.regex.Pattern;

/**
 * A PathMatcher implementation that provides strict POSIX-compliant glob pattern matching.
 * This implementation follows POSIX standards more strictly than the default Java NIO implementation.
 */
public class StrictPosixGlobPathMatcher implements PathMatcher {
    private final PathMatcher delegate;
    private final Pattern pattern;
    private final String originalPattern;

    /**
     * Creates a new StrictPosixGlobPathMatcher with the specified glob pattern.
     *
     * @param globPattern the glob pattern to match against
     */
    public StrictPosixGlobPathMatcher(String globPattern) {
        this.originalPattern = globPattern;
        this.pattern = compileGlobPattern(globPattern);
        // We still use the default implementation as a fallback for complex cases
        this.delegate = FileSystems.getDefault().getPathMatcher("glob:" + globPattern);
    }

    @Override
    public boolean matches(Path path) {
        String pathString = path.toString();
        return pattern.matcher(pathString).matches();
    }

    /**
     * Compiles a glob pattern into a regular expression pattern following strict POSIX rules.
     *
     * @param globPattern the glob pattern to compile
     * @return a Pattern object representing the compiled glob pattern
     */
    private Pattern compileGlobPattern(String globPattern) {
        StringBuilder regex = new StringBuilder("^");
        boolean inCharClass = false;
        boolean escaped = false;

        for (int i = 0; i < globPattern.length(); i++) {
            char c = globPattern.charAt(i);

            if (escaped) {
                // Handle escaped character
                regex.append(Pattern.quote(String.valueOf(c)));
                escaped = false;
                continue;
            }

            switch (c) {
                case '\\':
                    escaped = true;
                    break;
                case '*':
                    if (i + 1 < globPattern.length() && globPattern.charAt(i + 1) == '*') {
                        // Handle ** (match across directories)
                        regex.append(".*");
                        i++; // Skip the next *
                    } else {
                        // Handle * (match within a directory)
                        regex.append("[^/]*");
                    }
                    break;
                case '?':
                    // Match exactly one character, but not a path separator
                    regex.append("[^/]");
                    break;
                case '[':
                    inCharClass = true;
                    regex.append('[');
                    break;
                case ']':
                    inCharClass = false;
                    regex.append(']');
                    break;
                case '{':
                    // Handle alternation
                    regex.append('(');
                    break;
                case '}':
                    regex.append(')');
                    break;
                case ',':
                    if (!inCharClass) {
                        // Comma outside character class is used for alternation
                        regex.append('|');
                    } else {
                        regex.append(',');
                    }
                    break;
                case '/':
                    // Path separator should be matched literally
                    regex.append('/');
                    break;
                case '.':
                    // Escape dot to match it literally
                    regex.append("\\.");
                    break;
                default:
                    // Add character as-is if it's not special
                    if ("[](){}+^$|\\".indexOf(c) != -1) {
                        regex.append('\\');
                    }
                    regex.append(c);
            }
        }

        regex.append('$');
        return Pattern.compile(regex.toString());
    }

    /**
     * Returns the original glob pattern used to create this matcher.
     *
     * @return the original glob pattern
     */
    public String getPattern() {
        return originalPattern;
    }
}
