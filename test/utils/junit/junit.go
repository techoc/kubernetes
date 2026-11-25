/*
Copyright 2017 The Kubernetes Authors.

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

// Package junit 提供数据结构，以便于对 JUnit 测试结果进行 XML 编码
// 和解码。
package junit

import (
	"encoding/xml"
	"time"
)

// TestSuite 是包含测试用例的顶层测试套件。
type TestSuite struct {
	XMLName xml.Name `xml:"testsuite"`

	Name      string    `xml:"name,attr"`
	Tests     int       `xml:"tests,attr"`
	Disabled  int       `xml:"disabled,attr,omitempty"`
	Errors    int       `xml:"errors,attr"`
	Failures  int       `xml:"failures,attr"`
	Skipped   int       `xml:"skipped,attr,omitempty"`
	Time      float64   `xml:"time,attr"`
	Timestamp time.Time `xml:"timestamp,attr"`
	ID        int       `xml:"id,attr,omitempty"`
	Package   string    `xml:"package,attr,omitempty"`
	Hostname  string    `xml:"hostname,attr"`

	Properties []*Property `xml:"properties,omitempty"`
	TestCases  []*TestCase `xml:"testcase"`

	SystemOut string `xml:"system-out,omitempty"`
	SystemErr string `xml:"system-err,omitempty"`
}

// Update 遍历 TestCases，并更新 Tests、Errors、
// Failures 和 Skipped 这些顶层属性。
func (t *TestSuite) Update() {
	t.Tests = len(t.TestCases)
	for _, tc := range t.TestCases {
		t.Errors += len(tc.Errors)
		t.Failures += len(tc.Failures)
		if len(tc.Skipped) > 0 {
			t.Skipped++
		}
	}
}

// Property 是可以附加到 TestSuite 的简单键值对属性。
type Property struct {
	XMLName xml.Name `xml:"property"`

	Name  string `xml:"name,attr"`
	Value string `xml:"value,attr"`
}

// Error 表示测试用例中的错误。
type Error struct {
	XMLName xml.Name `xml:"error"`

	Message string `xml:"message,attr,omitempty"`
	Type    string `xml:"type,attr"`

	Value string `xml:",cdata"`
}

// Failure 表示测试用例中的失败。
type Failure struct {
	XMLName xml.Name `xml:"failure"`

	Message string `xml:"message,attr,omitempty"`
	Type    string `xml:"type,attr"`

	Value string `xml:",cdata"`
}

// TestCase 表示套件中的单个测试用例。
type TestCase struct {
	XMLName xml.Name `xml:"testcase"`

	Name       string  `xml:"name,attr"`
	Classname  string  `xml:"classname,attr"`
	Status     string  `xml:"status,attr,omitempty"`
	Assertions int     `xml:"assertions,attr,omitempty"`
	Time       float64 `xml:"time,attr"`

	Skipped string `xml:"skipped,omitempty"`

	Errors   []*Error   `xml:"error,omitempty"`
	Failures []*Failure `xml:"failure,omitempty"`
}
