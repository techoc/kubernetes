/*Package junitxml 根据 testjson.Execution 生成 JUnit XML 报告。
 */
package junitxml

import (
	"encoding/xml"
)

// JUnitTestSuites 是若干 JUnit 测试集的集合。
type JUnitTestSuites struct {
	XMLName xml.Name         `xml:"testsuites"`
	Suites  []JUnitTestSuite `xml:"testsuite,omitempty"`
}

// JUnitTestSuite 表示一个测试集，其中可包含多个测试用例。
type JUnitTestSuite struct {
	XMLName    xml.Name        `xml:"testsuite"`
	Tests      int             `xml:"tests,attr"`
	Failures   int             `xml:"failures,attr"`
	Time       string          `xml:"time,attr"`
	Name       string          `xml:"name,attr"`
	Properties []JUnitProperty `xml:"properties>property,omitempty"`
	TestCases  []JUnitTestCase `xml:"testcase,omitempty"`
	Timestamp  string          `xml:"timestamp,attr"`
}

// JUnitTestCase 表示单个测试用例及其结果。
type JUnitTestCase struct {
	XMLName     xml.Name          `xml:"testcase"`
	Classname   string            `xml:"classname,attr"`
	Name        string            `xml:"name,attr"`
	Time        string            `xml:"time,attr"`
	SkipMessage *JUnitSkipMessage `xml:"skipped,omitempty"`
	Failure     *JUnitFailure     `xml:"failure,omitempty"`
	SystemOut   string            `xml:"system-out,omitempty"`
	SystemErr   string            `xml:"system-err,omitempty"`
}

// JUnitSkipMessage 记录测试用例被跳过的原因。
type JUnitSkipMessage struct {
	Message string `xml:"message,attr"`
}

// JUnitProperty 表示用于定义属性的键值对。
type JUnitProperty struct {
	Name  string `xml:"name,attr"`
	Value string `xml:"value,attr"`
}

// JUnitFailure 保存与失败测试相关的数据。
type JUnitFailure struct {
	Message  string `xml:"message,attr"`
	Type     string `xml:"type,attr"`
	Contents string `xml:",chardata"`
}
