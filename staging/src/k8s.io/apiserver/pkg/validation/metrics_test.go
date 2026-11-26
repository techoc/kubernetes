/*
Copyright 2025 The Kubernetes Authors.

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

package validation

import (
	"strings"
	"testing"

	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/component-base/metrics/testutil"
)

// 测试用的校验标识，用于区分同一指标下的不同标签值。
const testIdentifier = "test_validation_identifier"
const anotherTestIdentifier = "another_test_validation_identifier"

// TestDeclarativeValidationMismatchMetric 验证 mismatch 指标每次正确递增 1。
//
// 每次递增应同时影响“按标识细分”的 alpha 指标与“总量”beta 指标。
func TestDeclarativeValidationMismatchMetric(t *testing.T) {
	defer legacyregistry.Reset()
	defer ResetValidationMetricsInstance()

	// 指标递增一次。
	Metrics.IncDeclarativeValidationMismatchMetric(testIdentifier)

	expected := `
	# HELP apiserver_validation_declarative_validation_parity_discrepancies_total [ALPHA] Number of discrepancies between declarative and handwritten validation, broken down by validation identifier.
	# TYPE apiserver_validation_declarative_validation_parity_discrepancies_total counter
	apiserver_validation_declarative_validation_parity_discrepancies_total{validation_identifier="test_validation_identifier"} 1
	# HELP apiserver_validation_declarative_validation_mismatch_total [BETA] Number of times declarative validation results differed from handwritten validation results for core types.
	# TYPE apiserver_validation_declarative_validation_mismatch_total counter
	apiserver_validation_declarative_validation_mismatch_total 1
	
	`

	if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(expected), "apiserver_validation_declarative_validation_parity_discrepancies_total", "apiserver_validation_declarative_validation_mismatch_total"); err != nil {
		t.Fatal(err)
	}
}

// TestDeclarativeValidationPanicMetric 验证 panic 指标每次正确递增 1。
//
// 与 mismatch 同理：panic 也需同时体现在细分与总量两个指标上。
func TestDeclarativeValidationPanicMetric(t *testing.T) {
	defer legacyregistry.Reset()
	defer ResetValidationMetricsInstance()

	// 指标递增一次。
	Metrics.IncDeclarativeValidationPanicMetric(testIdentifier)

	expected := `
	# HELP apiserver_validation_declarative_validation_panics_total [ALPHA] Number of panics in declarative validation, broken down by validation identifier.
	# TYPE apiserver_validation_declarative_validation_panics_total counter
	apiserver_validation_declarative_validation_panics_total{validation_identifier="test_validation_identifier"} 1
	# HELP apiserver_validation_declarative_validation_panic_total [BETA] Number of times declarative validation has panicked during validation.
	# TYPE apiserver_validation_declarative_validation_panic_total counter
	apiserver_validation_declarative_validation_panic_total 1
	`

	if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(expected),
		"apiserver_validation_declarative_validation_panic_total", "apiserver_validation_declarative_validation_panics_total"); err != nil {
		t.Fatal(err)
	}
}

// TestDuplicateValidationErrorMetric 验证 duplicate error 指标每次正确递增 1。
//
// 重复错误本身没有标签维度，只验证内部指标（INTERNAL）的计数。
func TestDuplicateValidationErrorMetric(t *testing.T) {
	defer legacyregistry.Reset()
	defer ResetValidationMetricsInstance()

	// 指标递增一次。
	Metrics.IncDuplicateValidationErrorMetric()

	expected := `
	# HELP apiserver_validation_duplicate_validation_error_total [INTERNAL] Number of duplicate validation errors during validation.
	# TYPE apiserver_validation_duplicate_validation_error_total counter
	apiserver_validation_duplicate_validation_error_total 1
	`

	if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(expected), "apiserver_validation_duplicate_validation_error_total"); err != nil {
		t.Fatal(err)
	}
}

// TestDeclarativeValidationMismatchMetricMultiple 验证 mismatch 指标多次递增正确。
//
// 同时使用两个不同标识，验证按标签累计（2/1）与总量累计（3）互不干扰。
func TestDeclarativeValidationMismatchMetricMultiple(t *testing.T) {
	defer legacyregistry.Reset()
	defer ResetValidationMetricsInstance()

	// 指标递增三次。
	Metrics.IncDeclarativeValidationMismatchMetric(testIdentifier)
	Metrics.IncDeclarativeValidationMismatchMetric(testIdentifier)
	Metrics.IncDeclarativeValidationMismatchMetric(anotherTestIdentifier)

	expected := `
	# HELP apiserver_validation_declarative_validation_parity_discrepancies_total [ALPHA] Number of discrepancies between declarative and handwritten validation, broken down by validation identifier.
	# TYPE apiserver_validation_declarative_validation_parity_discrepancies_total counter
	apiserver_validation_declarative_validation_parity_discrepancies_total{validation_identifier="test_validation_identifier"} 2
	apiserver_validation_declarative_validation_parity_discrepancies_total{validation_identifier="another_test_validation_identifier"} 1
	# HELP apiserver_validation_declarative_validation_mismatch_total [BETA] Number of times declarative validation results differed from handwritten validation results for core types.
	# TYPE apiserver_validation_declarative_validation_mismatch_total counter
	apiserver_validation_declarative_validation_mismatch_total 3
	`

	if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(expected), "apiserver_validation_declarative_validation_parity_discrepancies_total", "apiserver_validation_declarative_validation_mismatch_total"); err != nil {
		t.Fatal(err)
	}
}

// TestDeclarativeValidationPanicMetricMultiple 验证 panic 指标多次递增正确。
//
// 与 mismatch 的多标识累计场景对称。
func TestDeclarativeValidationPanicMetricMultiple(t *testing.T) {
	defer legacyregistry.Reset()
	defer ResetValidationMetricsInstance()

	// 指标递增三次。
	Metrics.IncDeclarativeValidationPanicMetric(testIdentifier)
	Metrics.IncDeclarativeValidationPanicMetric(testIdentifier)
	Metrics.IncDeclarativeValidationPanicMetric(anotherTestIdentifier)

	expected := `
	# HELP apiserver_validation_declarative_validation_panics_total [ALPHA] Number of panics in declarative validation, broken down by validation identifier.
	# TYPE apiserver_validation_declarative_validation_panics_total counter
	apiserver_validation_declarative_validation_panics_total{validation_identifier="test_validation_identifier"} 2
	apiserver_validation_declarative_validation_panics_total{validation_identifier="another_test_validation_identifier"} 1
	# HELP apiserver_validation_declarative_validation_panic_total [BETA] Number of times declarative validation has panicked during validation.
	# TYPE apiserver_validation_declarative_validation_panic_total counter
	apiserver_validation_declarative_validation_panic_total 3
	`

	if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(expected), "apiserver_validation_declarative_validation_panic_total", "apiserver_validation_declarative_validation_panics_total"); err != nil {
		t.Fatal(err)
	}
}

// TestDuplicateValidationErrorMetricMultiple 验证 duplicate error 指标多次递增正确。
func TestDuplicateValidationErrorMetricMultiple(t *testing.T) {
	defer legacyregistry.Reset()
	defer ResetValidationMetricsInstance()

	// 指标递增三次。
	Metrics.IncDuplicateValidationErrorMetric()
	Metrics.IncDuplicateValidationErrorMetric()
	Metrics.IncDuplicateValidationErrorMetric()

	expected := `
	# HELP apiserver_validation_duplicate_validation_error_total [INTERNAL] Number of duplicate validation errors during validation.
	# TYPE apiserver_validation_duplicate_validation_error_total counter
	apiserver_validation_duplicate_validation_error_total 3
	`

	if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(expected), "apiserver_validation_duplicate_validation_error_total"); err != nil {
		t.Fatal(err)
	}
}

// TestDeclarativeValidationMetricsReset 验证 Reset 函数能把指标正确归零。
//
// 先归零再重新累加，验证 Reset 之后计数器仍可正常使用（不会被置为不可用状态）。
func TestDeclarativeValidationMetricsReset(t *testing.T) {
	defer legacyregistry.Reset()
	defer ResetValidationMetricsInstance()

	// 让两个指标都递增。
	Metrics.IncDeclarativeValidationMismatchMetric(testIdentifier)
	Metrics.IncDeclarativeValidationPanicMetric(testIdentifier)
	Metrics.IncDuplicateValidationErrorMetric()

	// 重置指标。
	Metrics.Reset()

	// 验证它们已被归零。
	expected := `
	# HELP apiserver_validation_declarative_validation_mismatch_total [BETA] Number of times declarative validation results differed from handwritten validation results for core types.
	# TYPE apiserver_validation_declarative_validation_mismatch_total counter
	apiserver_validation_declarative_validation_mismatch_total 0
	# HELP apiserver_validation_declarative_validation_panic_total [BETA] Number of times declarative validation has panicked during validation.
	# TYPE apiserver_validation_declarative_validation_panic_total counter
	apiserver_validation_declarative_validation_panic_total 0
	# HELP apiserver_validation_duplicate_validation_error_total [INTERNAL] Number of duplicate validation errors during validation.
	# TYPE apiserver_validation_duplicate_validation_error_total counter
	apiserver_validation_duplicate_validation_error_total 0
	`

	if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(expected),
		"apiserver_validation_declarative_validation_mismatch_total",
		"apiserver_validation_declarative_validation_panic_total",
		"apiserver_validation_duplicate_validation_error_total"); err != nil {
		t.Fatal(err)
	}

	// 再次递增指标，确认它们仍然可用。
	Metrics.IncDeclarativeValidationMismatchMetric(testIdentifier)
	Metrics.IncDeclarativeValidationPanicMetric(testIdentifier)
	Metrics.IncDuplicateValidationErrorMetric()

	// 验证递增结果正确。
	expected = `
	# HELP apiserver_validation_declarative_validation_mismatch_total [BETA] Number of times declarative validation results differed from handwritten validation results for core types.
	# TYPE apiserver_validation_declarative_validation_mismatch_total counter
	apiserver_validation_declarative_validation_mismatch_total 1
	# HELP apiserver_validation_declarative_validation_panic_total [BETA] Number of times declarative validation has panicked during validation.
	# TYPE apiserver_validation_declarative_validation_panic_total counter
	apiserver_validation_declarative_validation_panic_total 1
	# HELP apiserver_validation_duplicate_validation_error_total [INTERNAL] Number of duplicate validation errors during validation.
	# TYPE apiserver_validation_duplicate_validation_error_total counter
	apiserver_validation_duplicate_validation_error_total 1
	# HELP apiserver_validation_declarative_validation_parity_discrepancies_total [ALPHA] Number of discrepancies between declarative and handwritten validation, broken down by validation identifier.
	# TYPE apiserver_validation_declarative_validation_parity_discrepancies_total counter
	apiserver_validation_declarative_validation_parity_discrepancies_total{validation_identifier="test_validation_identifier"} 1
	# HELP apiserver_validation_declarative_validation_panics_total [ALPHA] Number of panics in declarative validation, broken down by validation identifier.
	# TYPE apiserver_validation_declarative_validation_panics_total counter
	apiserver_validation_declarative_validation_panics_total{validation_identifier="test_validation_identifier"} 1
	`

	if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(expected),
		"apiserver_validation_declarative_validation_mismatch_total",
		"apiserver_validation_declarative_validation_parity_discrepancies_total",
		"apiserver_validation_declarative_validation_panic_total",
		"apiserver_validation_declarative_validation_panics_total",
		"apiserver_validation_duplicate_validation_error_total"); err != nil {
		t.Fatal(err)
	}
}
