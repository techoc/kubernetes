/*
Copyright 2018 The Kubernetes Authors.

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

package coverage

import (
	"io"
	"reflect"
	"time"
)

// fakeTestDeps 是 testing.testDeps 的实现。由于并不真正运行测试，这些方法可以什么都不做；
// 但至少要提供 ImportPath 的具体实现，因为运行测试时它会被无条件调用。
//
//nolint:unused // U1000 见上方说明，正常情况下确实未被使用。
type fakeTestDeps struct{}

// corpusEntry 的类型定义与 testing 包内部保持一致，见 https://go.dev/src/testing/fuzz.go#L88
//
//nolint:unused // U1000 见上方说明，正常情况下确实未被使用。
type corpusEntry = struct {
	Parent     string
	Path       string
	Data       []byte
	Values     []any
	Generation int
	IsSeed     bool
}

//nolint:unused // U1000 见上方说明，正常情况下确实未被使用。
func (fakeTestDeps) ImportPath() string {
	return ""
}

//nolint:unused // U1000 见上方说明，正常情况下确实未被使用。
func (fakeTestDeps) ModulePath() string {
	return ""
}

//nolint:unused // U1000 见上方说明，正常情况下确实未被使用。
func (fakeTestDeps) MatchString(pat, str string) (bool, error) {
	return false, nil
}

//nolint:unused // U1000 见上方说明，正常情况下确实未被使用。
func (fakeTestDeps) SetPanicOnExit0(bool) {}

//nolint:unused // U1000 见上方说明，正常情况下确实未被使用。
func (fakeTestDeps) StartCPUProfile(io.Writer) error {
	return nil
}

//nolint:unused // U1000 见上方说明，正常情况下确实未被使用。
func (fakeTestDeps) StopCPUProfile() {}

//nolint:unused // U1000 见上方说明，正常情况下确实未被使用。
func (fakeTestDeps) StartTestLog(io.Writer) {}

//nolint:unused // U1000 见上方说明，正常情况下确实未被使用。
func (fakeTestDeps) StopTestLog() error {
	return nil
}

//nolint:unused // U1000 见上方说明，正常情况下确实未被使用。
func (fakeTestDeps) WriteHeapProfile(io.Writer) error {
	return nil
}

//nolint:unused // U1000 见上方说明，正常情况下确实未被使用。
func (fakeTestDeps) WriteProfileTo(string, io.Writer, int) error {
	return nil
}

//nolint:unused // U1000 见上方说明，正常情况下确实未被使用。
func (fakeTestDeps) CoordinateFuzzing(time.Duration, int64, time.Duration, int64, int, []corpusEntry, []reflect.Type, string, string) error {
	return nil
}

//nolint:unused // U1000 见上方说明，正常情况下确实未被使用。
func (fakeTestDeps) RunFuzzWorker(func(corpusEntry) error) error {
	return nil
}

//nolint:unused // U1000 见上方说明，正常情况下确实未被使用。
func (fakeTestDeps) ReadCorpus(string, []reflect.Type) ([]corpusEntry, error) {
	return nil, nil
}

//nolint:unused // U1000 见上方说明，正常情况下确实未被使用。
func (fakeTestDeps) CheckCorpus([]any, []reflect.Type) error {
	return nil
}

//nolint:unused // U1000 见上方说明，正常情况下确实未被使用。
func (fakeTestDeps) ResetCoverage() {}

//nolint:unused // U1000 见上方说明，正常情况下确实未被使用。
func (fakeTestDeps) SnapshotCoverage() {}

//nolint:unused // U1000 见上方说明，正常情况下确实未被使用。
func (fakeTestDeps) InitRuntimeCoverage() (mode string, tearDown func(string, string) (string, error), snapcov func() float64) {
	return "", nil, nil
}
