/*
Copyright 2014 The Kubernetes Authors.

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

package yaml

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

// TestYAMLDecoderReadBytesLength 验证 DocumentDecoder.Read 的契约：
// 单个 YAML 文档超过缓冲区时返回 io.ErrShortBuffer 并给出已读长度，
// 缓冲区足够大时一次读完整篇文档。
func TestYAMLDecoderReadBytesLength(t *testing.T) {
	d := `---
stuff: 1
	test-foo: 1
`
	testCases := []struct {
		bufLen    int
		expectLen int
		expectErr error
	}{
		{len(d), len(d), nil},
		{len(d) + 10, len(d), nil},
		{len(d) - 10, len(d) - 10, io.ErrShortBuffer},
	}

	for i, testCase := range testCases {
		r := NewDocumentDecoder(io.NopCloser(bytes.NewReader([]byte(d))))
		b := make([]byte, testCase.bufLen)
		n, err := r.Read(b)
		if err != testCase.expectErr || n != testCase.expectLen {
			t.Fatalf("%d: unexpected body: %d / %v", i, n, err)
		}
	}
}

// TestBigYAML 覆盖大文档的两条边界：
// 缓冲区小于文档 -> ErrShortBuffer；超过 5MB 上限 -> bufio.ErrTooLong。
func TestBigYAML(t *testing.T) {
	d := `
stuff: 1
`
	maxLen := 5 * 1024 * 1024
	bufferLen := 4 * 1024
	//  maxLen 5 M
	dd := strings.Repeat(d, 512*1024)
	r := NewDocumentDecoder(io.NopCloser(bytes.NewReader([]byte(dd[:maxLen-1]))))
	b := make([]byte, bufferLen)
	n, err := r.Read(b)
	if err != io.ErrShortBuffer {
		t.Fatalf("expected ErrShortBuffer: %d / %v", n, err)
	}
	b = make([]byte, maxLen)
	n, err = r.Read(b)
	if err != nil {
		t.Fatalf("expected nil: %d / %v", n, err)
	}
	r = NewDocumentDecoder(io.NopCloser(bytes.NewReader([]byte(dd))))
	b = make([]byte, maxLen)
	n, err = r.Read(b)
	if err != bufio.ErrTooLong {
		t.Fatalf("bufio.Scanner: token too long: %d / %v", n, err)
	}
}

// TestYAMLDecoderCallsAfterErrShortBufferRestOfFrame 验证 ErrShortBuffer 之后
// 继续 Read 能拿到文档剩余部分，而不是从头重来或丢失数据。
func TestYAMLDecoderCallsAfterErrShortBufferRestOfFrame(t *testing.T) {
	d := `---
stuff: 1
	test-foo: 1`
	r := NewDocumentDecoder(io.NopCloser(bytes.NewReader([]byte(d))))
	b := make([]byte, 12)
	n, err := r.Read(b)
	if err != io.ErrShortBuffer || n != 12 {
		t.Fatalf("expected ErrShortBuffer: %d / %v", n, err)
	}
	expected := "---\nstuff: 1"
	if string(b) != expected {
		t.Fatalf("expected bytes read to be: %s  got: %s", expected, string(b))
	}
	b = make([]byte, 13)
	n, err = r.Read(b)
	if err != nil || n != 13 {
		t.Fatalf("expected nil: %d / %v", n, err)
	}
	expected = "\n\ttest-foo: 1"
	if string(b) != expected {
		t.Fatalf("expected bytes read to be: '%s'  got: '%s'", expected, string(b))
	}
	b = make([]byte, 15)
	n, err = r.Read(b)
	if err != io.EOF || n != 0 { //nolint:errorlint
		t.Fatalf("expected EOF: %d / %v", n, err)
	}
}

// TestSplitYAMLDocument 验证 bufio.SplitFunc 的语义：
// 只有遇到行首的 "---" 才切分；atEOF 为 false 时末尾不完整的内容必须等待更多数据，
// 因此返回 adv=0（不消耗）而不是提前切出一个不完整的文档。
func TestSplitYAMLDocument(t *testing.T) {
	testCases := []struct {
		input  string
		atEOF  bool
		expect string
		adv    int
	}{
		{"foo", true, "foo", 3},
		{"fo", false, "", 0},

		{"---", true, "---", 3},
		{"---\n", true, "---\n", 4},
		{"---\n", false, "", 0},

		{"\n---\n", false, "", 5},
		{"\n---\n", true, "", 5},

		{"abc\n---\ndef", true, "abc", 8},
		{"def", true, "def", 3},
		{"", true, "", 0},
	}
	for i, testCase := range testCases {
		adv, token, err := splitYAMLDocument([]byte(testCase.input), testCase.atEOF)
		if err != nil {
			t.Errorf("%d: unexpected error: %v", i, err)
			continue
		}
		if adv != testCase.adv {
			t.Errorf("%d: advance did not match: %d %d", i, testCase.adv, adv)
		}
		if testCase.expect != string(token) {
			t.Errorf("%d: token did not match: %q %q", i, testCase.expect, string(token))
		}
	}
}

// TestGuessJSON 验证 GuessJSONStream 会跳过前导空白再判断首字符，
// 并把完整内容（含空白）留给返回的 reader。
func TestGuessJSON(t *testing.T) {
	if r, _, isJSON := GuessJSONStream(bytes.NewReader([]byte(" \n{}")), 100); !isJSON {
		t.Fatalf("expected stream to be JSON")
	} else {
		b := make([]byte, 30)
		n, err := r.Read(b)
		if err != nil || n != 4 {
			t.Fatalf("unexpected body: %d / %v", n, err)
		}
		if string(b[:n]) != " \n{}" {
			t.Fatalf("unexpected body: %q", string(b[:n]))
		}
	}
}

// TestScanYAML 验证 splitYAMLDocument 可直接作为 bufio.Scanner 的 SplitFunc 使用：
// 两个文档应产生两次 Scan，之后扫描结束且无错误。
func TestScanYAML(t *testing.T) {
	s := bufio.NewScanner(bytes.NewReader([]byte(`---
stuff: 1

---
  `)))
	s.Split(splitYAMLDocument)
	if !s.Scan() {
		t.Fatalf("should have been able to scan")
	}
	t.Logf("scan: %s", s.Text())
	if !s.Scan() {
		t.Fatalf("should have been able to scan")
	}
	t.Logf("scan: %s", s.Text())
	if s.Scan() {
		t.Fatalf("scan should have been done")
	}
	if s.Err() != nil {
		t.Fatalf("err should have been nil: %v", s.Err())
	}
}

// TestDecodeYAML 验证 YAMLToJSONDecoder 能连续解码多个文档，
// 空文档解码为空对象，流结束后返回 io.EOF。
func TestDecodeYAML(t *testing.T) {
	s := NewYAMLToJSONDecoder(bytes.NewReader([]byte(`---
stuff: 1

---   
  `)))
	obj := generic{}
	if err := s.Decode(&obj); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fmt.Sprintf("%#v", obj) != `yaml.generic{"stuff":1}` {
		t.Errorf("unexpected object: %#v", obj)
	}
	obj = generic{}
	if err := s.Decode(&obj); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(obj) != 0 {
		t.Fatalf("unexpected object: %#v", obj)
	}
	obj = generic{}
	if err := s.Decode(&obj); err != io.EOF { //nolint:errorlint
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestDecodeYAMLSeparatorValidation 验证分隔行：
// "---  # 注释" 合法；"--- 后面跟未注释内容" 应报 YAMLSyntaxError。
func TestDecodeYAMLSeparatorValidation(t *testing.T) {
	s := NewYAMLToJSONDecoder(bytes.NewReader([]byte(`---
stuff: 1
---    # Make sure termination happen with inline comment
stuff: 2
---
stuff: 3
--- Make sure uncommented content results YAMLSyntaxError

 `)))
	obj := generic{}
	if err := s.Decode(&obj); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fmt.Sprintf("%#v", obj) != `yaml.generic{"stuff":1}` {
		t.Errorf("unexpected object: %#v", obj)
	}
	obj = generic{}
	if err := s.Decode(&obj); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fmt.Sprintf("%#v", obj) != `yaml.generic{"stuff":2}` {
		t.Errorf("unexpected object: %#v", obj)
	}
	obj = generic{}
	err := s.Decode(&obj)
	if err == nil {
		t.Fatalf("expected YamlSyntaxError, got nil instead")
	}
	if _, ok := err.(YAMLSyntaxError); !ok {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestDecodeBrokenYAML 验证 YAML 语法错误会带上行号，便于用户定位缩进问题。
func TestDecodeBrokenYAML(t *testing.T) {
	s := NewYAMLOrJSONDecoder(bytes.NewReader([]byte(`---
stuff: 1
		test-foo: 1

---
  `)), 100)
	obj := generic{}
	err := s.Decode(&obj)
	if err == nil {
		t.Fatal("expected error with yaml: violate, got no error")
	}
	fmt.Printf("err: %s\n", err.Error())
	if !strings.Contains(err.Error(), "yaml: line 3:") {
		t.Fatalf("expected %q to have 'yaml: line 3:' found a tab character", err.Error())
	}
}

// TestDecodeBrokenJSON 验证损坏的 JSON 会返回带 json 前缀的语法错误。
func TestDecodeBrokenJSON(t *testing.T) {
	s := NewYAMLOrJSONDecoder(bytes.NewReader([]byte(`{
	"foo": {
		"stuff": 1
		"otherStuff": 2
	}
}
  `)), 100)
	obj := generic{}
	err := s.Decode(&obj)
	if err == nil {
		t.Fatal("expected error with json: prefix, got no error")
	}
	const msg = `json: .*invalid character.*".*after object key:value pair`
	if matched, _ := regexp.MatchString(msg, err.Error()); !matched {
		t.Fatalf("expected string matching %q, got %q", msg, err.Error())
	}
}

// generic 是测试用的宽松 JSON/YAML 对象，便于直接比较解码结果。
type generic map[string]interface{}

// TestYAMLOrJSONDecoder 覆盖自动嗅探 JSON/YAML 的各种组合：
// 缓冲区大小对嗅探的影响、连续 JSON 文档、混合格式流，以及首文档决定整体格式的规则。
func TestYAMLOrJSONDecoder(t *testing.T) {
	testCases := []struct {
		input  string
		buffer int
		isJSON bool
		err    bool
		out    []generic
	}{
		// 相邻的两个 JSON 对象无需分隔符即可连续解码。
		{` {"1":2}{"3":4}`, 2, true, false, []generic{
			{"1": 2},
			{"3": 4},
		}},
		{" \n{}", 3, true, false, []generic{
			{},
		}},
		{" \na: b", 2, false, false, []generic{
			{"a": "b"},
		}},
		// 缓冲区只有 2 字节，嗅探时看不到完整 JSON 开头，回退为 YAML 并失败。
		{" \n{\"a\": \"b\"}", 2, false, true, []generic{
			{"a": "b"},
		}},
		{" \n{\"a\": \"b\"}", 3, true, false, []generic{
			{"a": "b"},
		}},
		{`   {"a":"b"}`, 100, true, false, []generic{
			{"a": "b"},
		}},
		{"", 1, false, false, []generic{}},
		{"foo: bar\n---\nbaz: biz", 100, false, false, []generic{
			{"foo": "bar"},
			{"baz": "biz"},
		}},
		{"---\nfoo: bar\n--- # with Comment\nbaz: biz", 100, false, false, []generic{
			{"foo": "bar"},
			{"baz": "biz"},
		}},
		//YAML 缩进允许空格，不允许制表符。
		{"foo:\n  field: bar\n---\nbaz:\n  field: biz", 100, false, false, []generic{
			{"foo": map[string]any{"field": "bar"}},
			{"baz": map[string]any{"field": "biz"}},
		}},
		{"foo: bar\n---\n", 100, false, false, []generic{
			{"foo": "bar"},
		}},
		{"foo: bar\n---", 100, false, false, []generic{
			{"foo": "bar"},
		}},
		{"foo: bar\n--", 100, false, true, []generic{
			{"foo": "bar"},
		}},
		{"foo: bar\n-", 100, false, true, []generic{
			{"foo": "bar"},
		}},
		{"foo: bar\n", 100, false, false, []generic{
			{"foo": "bar"},
		}},
		//第一个文档是 JSON，第二个是 YAML。
		{"{\"foo\": \"bar\"}\n---\n{baz: biz}", 100, false, false, []generic{
			{"foo": "bar"},
			{"baz": "biz"},
		}},
		//第一个文档是 JSON，第二个是更小的 YAML。
		{"{\"foo\": \"bar\"}\n---\na: b", 100, false, false, []generic{
			{"foo": "bar"},
			{"a": "b"},
		}},
		//第一个文档是 JSON，第二个是更小的 YAML，且
		//末尾带空白。
		{"{\"foo\": \"bar\"}    \n---\na: b", 100, false, false, []generic{
			{"foo": "bar"},
			{"a": "b"},
		}},
		//第一个文档是 JSON，第二个 YAML 超过缓冲区大小。
		{"{\"foo\": \"bar\"}\n---\n{baz: biz0123456780123456780123456780123456780123456789}", 20, false, false, []generic{
			{"foo": "bar"},
			{"baz": "biz0123456780123456780123456780123456780123456789"},
		}},
		//第一个文档是 JSON，接着空白，然后是 YAML。
		{"{\"foo\": \"bar\"}    \n---\n{baz: biz}", 100, false, false, []generic{
			{"foo": "bar"},
			{"baz": "biz"},
		}},
		//第一个文档是 YAML，第二个是 JSON。
		{"{foo: bar}\n---\n{\"baz\": \"biz\"}", 100, false, false, []generic{
			{"foo": "bar"},
			{"baz": "biz"},
		}},
		//第一个文档是 JSON，第二个是使用空格缩进的 YAML。
		{"{\n  \"foo\": \"bar\"\n}\n---\n{\n  baz: biz\n}", 100, false, false, []generic{
			{"foo": "bar"},
			{"baz": "biz"},
		}},
		//第一个文档是 JSON，第二个是使用制表符缩进的 YAML。
		{"{\n\t\"foo\": \"bar\"\n}\n---\n{\n\tbaz: biz\n}", 100, false, false, []generic{
			{"foo": "bar"},
			{"baz": "biz"},
		}},
		//前两个文档是 JSON，第三个是 YAML（整个流按 JSON 处理）。
		// 格式由首个文档锁定：后续出现 YAML 会报错，而不是切换解码器。
		{"{\"foo\": \"bar\"}\n{\"baz\": \"biz\"}\n---\n{qux: zrb}", 100, true, true, nil},
	}
	for i, testCase := range testCases {
		decoder := NewYAMLOrJSONDecoder(bytes.NewReader([]byte(testCase.input)), testCase.buffer)
		objs := []generic{}

		// 循环解码直到出错；io.EOF 是正常结束，其他错误按 err 期望判断。
		var err error
		for {
			out := make(generic)
			err = decoder.Decode(&out)
			if err != nil {
				break
			}
			objs = append(objs, out)
		}
		if err != io.EOF { //nolint:errorlint
			switch {
			case testCase.err && err == nil:
				t.Errorf("%d: unexpected non-error", i)
				continue
			case !testCase.err && err != nil:
				t.Errorf("%d: unexpected error: %v", i, err)
				continue
			case err != nil:
				continue
			}
		}
		switch {
		case decoder.yaml != nil:
			if testCase.isJSON {
				t.Errorf("%d: expected JSON decoder, got YAML", i)
			}
		case decoder.json != nil:
			if !testCase.isJSON {
				t.Errorf("%d: expected YAML decoder, got JSON", i)
			}
		}
		if fmt.Sprintf("%#v", testCase.out) != fmt.Sprintf("%#v", objs) {
			t.Errorf("%d: objects were not equal: \n%#v\n%#v", i, testCase.out, objs)
		}
	}
}

// TestReadSingleLongLine 验证 LineReader 能读取超过 64KB 的单行（不依赖 bufio.Scanner 的默认上限）。
func TestReadSingleLongLine(t *testing.T) {
	testReadLines(t, []int{128 * 1024})
}

// TestReadRandomLineLengths 用随机行长覆盖 LineReader 在不同缓冲区边界下的行为。
func TestReadRandomLineLengths(t *testing.T) {
	minLength := 100
	maxLength := 96 * 1024
	maxLines := 100

	lineLengths := make([]int, maxLines)
	for i := 0; i < maxLines; i++ {
		lineLengths[i] = rand.Intn(maxLength-minLength) + minLength
	}

	testReadLines(t, lineLengths)
}

// testReadLines 构造随机内容的多行输入，验证 LineReader.Read 逐行原样返回。
func testReadLines(t *testing.T, lineLengths []int) {
	var (
		lines       [][]byte
		inputStream []byte
	)
	for _, lineLength := range lineLengths {
		inputLine := make([]byte, lineLength+1)
		for i := 0; i < lineLength; i++ {
			char := rand.Intn('z'-'A') + 'A'
			inputLine[i] = byte(char)
		}
		inputLine[len(inputLine)-1] = '\n'
		lines = append(lines, inputLine)
	}
	for _, line := range lines {
		inputStream = append(inputStream, line...)
	}

	//初始化 Reader
	reader := bufio.NewReader(bytes.NewReader(inputStream))
	lineReader := &LineReader{reader: reader}

	//读取各行
	var readLines [][]byte
	for range lines {
		bytes, err := lineReader.Read()
		if err != nil && err != io.EOF { //nolint:errorlint
			t.Fatalf("failed to read lines: %v", err)
		}
		readLines = append(readLines, bytes)
	}

	//校验
	for i := range lines {
		if len(lines[i]) != len(readLines[i]) {
			t.Fatalf("expected line length: %d, but got %d", len(lines[i]), len(readLines[i]))
		}
		if !reflect.DeepEqual(lines[i], readLines[i]) {
			t.Fatalf("expected line: %v, but got %v", lines[i], readLines[i])
		}
	}
}

// TestTypedJSONOrYamlErrors 验证错误被包装成可判定的具体类型
// （JSONSyntaxError/YAMLSyntaxError），调用方可以据此区分格式来源。
func TestTypedJSONOrYamlErrors(t *testing.T) {
	s := NewYAMLOrJSONDecoder(bytes.NewReader([]byte(`{
	"foo": {
		"stuff": 1
		"otherStuff": 2
	}
}
  `)), 100)
	obj := generic{}
	err := s.Decode(&obj)
	if err == nil {
		t.Fatal("expected error with json: prefix, got no error")
	}
	if _, ok := err.(JSONSyntaxError); !ok {
		t.Fatalf("expected %q to be of type JSONSyntaxError", err.Error())
	}

	s = NewYAMLOrJSONDecoder(bytes.NewReader([]byte(`---
stuff: 1
		test-foo: 1

---
  `)), 100)
	obj = generic{}
	err = s.Decode(&obj)
	if err == nil {
		t.Fatal("expected error with yaml: prefix, got no error")
	}
	if _, ok := err.(YAMLSyntaxError); !ok {
		t.Fatalf("expected %q to be of type YAMLSyntaxError", err.Error())
	}
}

// TestUnmarshal 验证 YAML 数字在目标为 interface{} 时统一解码为 int64；
// 但当 map 的键类型不是 string 时，数字会走 float64 路径（JSON 兼容行为）。
func TestUnmarshal(t *testing.T) {
	// map[string]interface{} 是 JSON 兼容路径，数字保持整数语义（int64）。
	mapWithIntegerBytes := []byte(`replicas: 1`)
	mapWithInteger := make(map[string]interface{})
	if err := Unmarshal(mapWithIntegerBytes, &mapWithInteger); err != nil {
		t.Fatalf("unexpected error unmarshaling yaml: %v", err)
	}
	if _, ok := mapWithInteger["replicas"].(int64); !ok {
		t.Fatalf(`Expected number in map to be int64 but got "%T"`, mapWithInteger["replicas"])
	}

	sliceWithIntegerBytes := []byte(`- 1`)
	var sliceWithInteger []interface{}
	if err := Unmarshal(sliceWithIntegerBytes, &sliceWithInteger); err != nil {
		t.Fatalf("unexpected error unmarshaling yaml: %v", err)
	}
	if _, ok := sliceWithInteger[0].(int64); !ok {
		t.Fatalf(`Expected number in slice to be int64 but got "%T"`, sliceWithInteger[0])
	}

	integerBytes := []byte(`1`)
	var integer interface{}
	if err := Unmarshal(integerBytes, &integer); err != nil {
		t.Fatalf("unexpected error unmarshaling yaml: %v", err)
	}
	if _, ok := integer.(int64); !ok {
		t.Fatalf(`Expected number to be int64 but got "%T"`, integer)
	}

	// 键类型为 int 时无法走 JSON 中转，退回原生 YAML 解码，数字变为 float64。
	otherTypeBytes := []byte(`123: 2`)
	otherType := make(map[int]interface{})
	if err := Unmarshal(otherTypeBytes, &otherType); err != nil {
		t.Fatalf("unexpected error unmarshaling yaml: %v", err)
	}
	if _, ok := otherType[123].(int64); ok {
		t.Fatalf(`Expected number not to be converted to int64`)
	}
	if _, ok := otherType[123].(float64); !ok {
		t.Fatalf(`Expected number to be float64 but got "%T"`, otherType[123])
	}
}
