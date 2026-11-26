/*
Copyright 2019 The Kubernetes Authors.

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

// 此工具用于检查一致性 e2e 测试是否遵守要求，参见
// https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/conformance-tests.md#conformance-test-requirements
package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"regexp"

	"github.com/spf13/cobra"
)

const (
	// 例如 framework.ConformanceIt("should provide secure master service", func(ctx context.Context) {
	patternStartConformance = `framework.ConformanceIt\(.*, func\(\) {$`
	patternEndConformance   = `}\)$`
	patternSkip             = `e2eskipper.Skip.*\(`
)

// checkAllProviders 检查"适用于所有 provider"这条要求，例如不得调用 SkipIfProviderIs/SkipUnlessProviderIs。
func checkAllProviders(e2eFile string) error {
	checkFailed := false
	inConformanceCode := false

	regStartConformance := regexp.MustCompile(patternStartConformance)
	regEndConformance := regexp.MustCompile(patternEndConformance)
	regSkip := regexp.MustCompile(patternSkip)

	fileInput, err := os.ReadFile(e2eFile)
	if err != nil {
		return fmt.Errorf("Failed to read file %s: %w", e2eFile, err)
	}
	scanner := bufio.NewScanner(bytes.NewReader(fileInput))
	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		line := scanner.Text()
		if regStartConformance.MatchString(line) {
			if inConformanceCode {
				return errors.New("Missed the end of previous conformance test. There might be a bug in this script.")
			}
			inConformanceCode = true
		}
		if inConformanceCode {
			if regSkip.MatchString(line) {
				// 为了一次运行列出所有不合规之处，这里不返回错误，而是继续检查。
				fmt.Fprintf(os.Stderr, "%v: Conformance test should not call any e2eskipper.Skip*()\n", e2eFile)
				checkFailed = true
			}
			if regEndConformance.MatchString(line) {
				inConformanceCode = false
			}
		}
	}
	if inConformanceCode {
		return errors.New("Missed the end of previous conformance test. There might be a bug in this script.")
	}
	if checkFailed {
		return errors.New("We need to fix the above errors.")
	}
	return nil
}

func processFile(e2ePath string) error {
	regGoFile := regexp.MustCompile(`.*\.go`)

	files, err := os.ReadDir(e2ePath)
	if err != nil {
		return fmt.Errorf("Failed to read dir %s: %w", e2ePath, err)
	}
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		if !regGoFile.MatchString(file.Name()) {
			continue
		}
		e2eFile := fmt.Sprintf("%s/%s", e2ePath, file.Name())
		err = checkAllProviders(e2eFile)
		if err != nil {
			return err
		}
	}
	return nil
}

func processDir(e2ePath string) error {
	err := processFile(e2ePath)
	if err != nil {
		return err
	}

	// 递归检查存在的子目录
	files, err := os.ReadDir(e2ePath)
	if err != nil {
		return fmt.Errorf("Failed to read dir %s: %w", e2ePath, err)
	}
	for _, file := range files {
		if !file.IsDir() {
			continue
		}
		err = processDir(fmt.Sprintf("%s/%s", e2ePath, file.Name()))
		if err != nil {
			return err
		}
	}
	return nil
}

func newCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "check_conformance_test_requirements [e2e-test-path]",
		Short: "Check conformance test code follows the requirements",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) != 1 {
				cmd.Help()
				os.Exit(1)
			}
			e2eRootPath := args[0]
			err := processDir(e2eRootPath)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				os.Exit(1)
			}
			os.Exit(0)
		},
	}
	return cmd
}

func main() {
	command := newCommand()
	if err := command.Execute(); err != nil {
		os.Exit(1)
	}
}
