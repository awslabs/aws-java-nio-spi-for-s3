/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3.util;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;

class StrictPosixGlobPathMatcherTest {

    @Test
    void testSimpleGlobMatching() {
        StrictPosixGlobPathMatcher matcher = new StrictPosixGlobPathMatcher("*.txt");
        
        assertThat(matcher.matches(Paths.get("file.txt"))).isTrue();
        assertThat(matcher.matches(Paths.get("file.csv"))).isFalse();
        assertThat(matcher.matches(Paths.get("dir/file.txt"))).isFalse(); // Should not match across directories
    }

    @Test
    void testDirectoryGlobMatching() {
        // Test directory matching with single asterisk
        StrictPosixGlobPathMatcher matcher = new StrictPosixGlobPathMatcher("dir/*/file.txt");
        
        assertThat(matcher.matches(Paths.get("dir/subdir/file.txt"))).isTrue();
        assertThat(matcher.matches(Paths.get("dir/file.txt"))).isFalse();
        assertThat(matcher.matches(Paths.get("dir/subdir/subsubdir/file.txt"))).isFalse();
    }

    @Test
    void testCharacterClassMatching() {
        StrictPosixGlobPathMatcher matcher = new StrictPosixGlobPathMatcher("file[0-9].txt");
        
        assertThat(matcher.matches(Paths.get("file1.txt"))).isTrue();
        assertThat(matcher.matches(Paths.get("file2.txt"))).isTrue();
        assertThat(matcher.matches(Paths.get("fileA.txt"))).isFalse();
    }

    @Test
    void testAlternationMatching() {
        StrictPosixGlobPathMatcher matcher = new StrictPosixGlobPathMatcher("{file,document}*.txt");
        
        assertThat(matcher.matches(Paths.get("file.txt"))).isTrue();
        assertThat(matcher.matches(Paths.get("file123.txt"))).isTrue();
        assertThat(matcher.matches(Paths.get("document.txt"))).isTrue();
        assertThat(matcher.matches(Paths.get("document123.txt"))).isTrue();
        assertThat(matcher.matches(Paths.get("other.txt"))).isFalse();
    }

    @Test
    void testQuestionMarkMatching() {
        StrictPosixGlobPathMatcher matcher = new StrictPosixGlobPathMatcher("file?.txt");
        
        assertThat(matcher.matches(Paths.get("fileA.txt"))).isTrue();
        assertThat(matcher.matches(Paths.get("file1.txt"))).isTrue();
        assertThat(matcher.matches(Paths.get("file.txt"))).isFalse();
        assertThat(matcher.matches(Paths.get("fileAB.txt"))).isFalse();
    }

    @Test
    void testEscapedCharacters() {
        StrictPosixGlobPathMatcher matcher = new StrictPosixGlobPathMatcher("file\\*.txt");
        
        assertThat(matcher.matches(Paths.get("file*.txt"))).isTrue();
        assertThat(matcher.matches(Paths.get("fileA.txt"))).isFalse();
    }
    
    @Test
    void testComplexCharacterClassMatching() {
        // Test character class with range
        StrictPosixGlobPathMatcher matcher = new StrictPosixGlobPathMatcher("file[A-Z].txt");
        
        assertThat(matcher.matches(Paths.get("fileA.txt"))).isTrue();
        assertThat(matcher.matches(Paths.get("fileZ.txt"))).isTrue();
        assertThat(matcher.matches(Paths.get("file1.txt"))).isFalse();
        
        // Test character class with individual characters
        StrictPosixGlobPathMatcher matcher2 = new StrictPosixGlobPathMatcher("file[xyz].txt");
        
        assertThat(matcher2.matches(Paths.get("filex.txt"))).isTrue();
        assertThat(matcher2.matches(Paths.get("filey.txt"))).isTrue();
        assertThat(matcher2.matches(Paths.get("filez.txt"))).isTrue();
        assertThat(matcher2.matches(Paths.get("filea.txt"))).isFalse();
    }
    
    @Test
    void testSpecialCharactersInPattern() {
        // Test dot character (should be treated as literal)
        StrictPosixGlobPathMatcher matcher = new StrictPosixGlobPathMatcher("file.txt");
        
        assertThat(matcher.matches(Paths.get("file.txt"))).isTrue();
        assertThat(matcher.matches(Paths.get("filetxt"))).isFalse();
        assertThat(matcher.matches(Paths.get("fileAtxt"))).isFalse();
        
        // Test with parentheses (should be treated as literals)
        StrictPosixGlobPathMatcher matcher2 = new StrictPosixGlobPathMatcher("file(test).txt");
        
        assertThat(matcher2.matches(Paths.get("file(test).txt"))).isTrue();
        assertThat(matcher2.matches(Paths.get("filetest.txt"))).isFalse();
        
        // Test with square brackets (should be treated as character class)
        StrictPosixGlobPathMatcher matcher3 = new StrictPosixGlobPathMatcher("file\\[test\\].txt");
        
        assertThat(matcher3.matches(Paths.get("file[test].txt"))).isTrue();
        assertThat(matcher3.matches(Paths.get("filet.txt"))).isFalse();
        
        // Test with other regex special characters
        StrictPosixGlobPathMatcher matcher4 = new StrictPosixGlobPathMatcher("file+^$|().txt");
        
        assertThat(matcher4.matches(Paths.get("file+^$|().txt"))).isTrue();
        assertThat(matcher4.matches(Paths.get("file.txt"))).isFalse();
    }
    
    @Test
    void testGetPattern() {
        String pattern = "*.txt";
        StrictPosixGlobPathMatcher matcher = new StrictPosixGlobPathMatcher(pattern);
        
        assertThat(matcher.getPattern()).isEqualTo(pattern);
        
        String complexPattern = "dir/**/[a-z]file?.txt";
        StrictPosixGlobPathMatcher complexMatcher = new StrictPosixGlobPathMatcher(complexPattern);
        
        assertThat(complexMatcher.getPattern()).isEqualTo(complexPattern);
    }
}
