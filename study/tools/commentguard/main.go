/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// CommentGuard checks that a comment-only translation left the code untouched.
// It compares token streams with comments stripped and protects machine-readable
// comments (directives, Example outputs, license headers) byte-for-byte.
package main

import (
	"flag"
	"fmt"
	"go/scanner"
	"go/token"
	"os"
	"os/exec"
	"regexp"
	"strings"
)

var (
	base    = flag.String("base", "HEAD", "git revision to compare against")
	files   = flag.String("files", "", "comma-separated list of files to check (default: all changed .go files)")
	verbose = flag.Bool("v", false, "print per-file results")
)

var (
	directiveRe = regexp.MustCompile(`^\s*//\s*(go:|nolint|\+)|^\s*//line\s|^\s*//\s*Deprecated:`)
	outputRe    = regexp.MustCompile(`^\s*//\s*(Unordered )?Output:`)
	licenseRe   = regexp.MustCompile(`Copyright (The )?Kubernetes Authors`)
)

type tokenInfo struct {
	kind token.Token
	text string
}

func tokens(src []byte) ([]tokenInfo, []string, []string, []string, error) {
	fset := token.NewFileSet()
	file := fset.AddFile("input", fset.Base(), len(src))
	var s scanner.Scanner
	// SkipComments would discard them entirely; we need the text for directives.
	s.Init(file, src, nil, scanner.ScanComments)

	var code []tokenInfo
	var directives, outputs, license []string
	inLicense := false
	for {
		_, tok, lit := s.Scan()
		if tok == token.EOF {
			break
		}
		if tok == token.COMMENT {
			raw := lit
			for _, line := range strings.Split(raw, "\n") {
				trimmed := strings.TrimRight(line, "\r")
				if licenseRe.MatchString(trimmed) {
					inLicense = true
				}
				if inLicense {
					license = append(license, trimmed)
					if strings.Contains(trimmed, "limitations under the License.") {
						inLicense = false
					}
					continue
				}
				if directiveRe.MatchString(trimmed) {
					directives = append(directives, trimmed)
				}
				if outputRe.MatchString(trimmed) {
					outputs = append(outputs, trimmed)
				}
			}
			continue
		}
		if lit == "" {
			lit = tok.String()
		}
		code = append(code, tokenInfo{tok, lit})
	}
	return code, directives, outputs, license, nil
}

func gitShow(rev, path string) ([]byte, error) {
	return exec.Command("git", "show", rev+":"+path).Output()
}

func summary(code []tokenInfo, directives, outputs, license []string) string {
	return fmt.Sprintf("code=%d directives=%d outputs=%d license=%d", len(code), len(directives), len(outputs), len(license))
}

func main() {
	flag.Parse()

	var targets []string
	if *files != "" {
		targets = strings.Split(*files, ",")
	} else {
		out, err := exec.Command("git", "diff", "--name-only", "--diff-filter=ACM", *base).Output()
		if err != nil {
			fmt.Fprintln(os.Stderr, "git diff:", err)
			os.Exit(2)
		}
		for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
			if strings.HasSuffix(line, ".go") {
				targets = append(targets, line)
			}
		}
	}

	failed := false
	for _, path := range targets {
		path = strings.TrimSpace(path)
		if path == "" {
			continue
		}
		newSrc, err := os.ReadFile(path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: read: %v\n", path, err)
			failed = true
			continue
		}
		oldSrc, err := gitShow(*base, path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: git show %s: %v\n", path, *base, err)
			failed = true
			continue
		}

		newCode, newDirs, newOutputs, newLicense, _ := tokens(newSrc)
		oldCode, oldDirs, oldOutputs, oldLicense, _ := tokens(oldSrc)

		var problems []string
		if len(newCode) != len(oldCode) {
			problems = append(problems, fmt.Sprintf("token count %d -> %d", len(oldCode), len(newCode)))
		} else {
			for i := range oldCode {
				if newCode[i] != oldCode[i] {
					problems = append(problems, fmt.Sprintf("token %d: %v -> %v", i, oldCode[i], newCode[i]))
					break
				}
			}
		}
		if strings.Join(newDirs, "\n") != strings.Join(oldDirs, "\n") {
			problems = append(problems, "machine-readable directives changed")
		}
		if strings.Join(newOutputs, "\n") != strings.Join(oldOutputs, "\n") {
			problems = append(problems, "Example Output lines changed")
		}
		if strings.Join(newLicense, "\n") != strings.Join(oldLicense, "\n") {
			problems = append(problems, "license header changed")
		}

		if len(problems) > 0 {
			failed = true
			fmt.Printf("FAIL %s: %s\n", path, strings.Join(problems, "; "))
		} else if *verbose {
			fmt.Printf("OK   %s (%s)\n", path, summary(newCode, newDirs, newOutputs, newLicense))
		}
	}

	if failed {
		os.Exit(1)
	}
	fmt.Printf("commentguard: %d file(s) passed\n", len(targets))
}
