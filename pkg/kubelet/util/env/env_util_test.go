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

package env

import (
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
)

// testShellBehavior 用 POSIX shell 解释给定的环境文件内容，
// 返回指定 key 最终被赋予的值。该辅助函数不在 Windows 上运行。
func testShellBehavior(t *testing.T, envContent, key string) (string, error) {
	tmpFile, err := os.CreateTemp("", "shell_test_*")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer func() { _ = os.Remove(tmpFile.Name()) }()

	if _, err := tmpFile.Write([]byte(envContent)); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	if err := tmpFile.Close(); err != nil {
		t.Fatalf("failed to close temp file: %v", err)
	}

	// 用 POSIX 模式的 bash 载入文件并打印变量值；
	// 使用 . 而不是 source，以保持 POSIX 兼容。
	cmd := exec.Command("bash", "--posix", "-c",
		"set -a && . \""+tmpFile.Name()+"\" && printf '%s' \"${"+key+"}\"")

	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", err
	}

	return string(output), nil
}

// TestParseEnv 以表驱动方式验证 ParseEnv 对单引号环境变量文件的解析：
// 覆盖空白/注释/空行、key 查找、引号与转义、多行值、特殊字符以及非法格式报错。
func TestParseEnv(t *testing.T) {
	tempDir := t.TempDir()

	// testCase 描述一个 ParseEnv 用例及其期望结果。
	type testCase struct {
		// name 是用例名称，用于 t.Run 的子测试名。
		name string
		// envContent 是写入临时环境文件的内容。
		envContent string
		// key 是要查询的环境变量名。
		key string
		// wantValue 是期望解析出的值。
		wantValue string
		// wantErr 表示该用例期望 ParseEnv 返回错误。
		wantErr bool
		// errContains 是期望错误信息包含的片段。
		errContains string
	}
	// 用例按语义分组：格式与空白处理、引号规则、特殊字符、
	// 多行内容，以及不应被展开的 shell 变量引用。
	tests := []testCase{
		{
			// 行首空白不影响 key 的识别。
			name: "ignore leading whitespace",
			envContent: `   KEY1='val1'
		KEY2='val2'
KEY3='val3'
`,
			key:       "KEY2",
			wantValue: `val2`,
		},
		{
			// 空行与 # 注释行直接跳过。
			name: "ignore blank and comment lines",
			envContent: `# comment

KEY1='foo'
   # another comment

KEY2='bar'
`,
			key:       "KEY2",
			wantValue: `bar`,
		},
		{
			// key 与 = 之间出现空白即视为非法格式。
			name: "whitespace around = and not allowed",
			envContent: `KEY1 = 'val1'
KEY2=   'val2'
KEY3='val3'
`,
			key:         "KEY2",
			wantErr:     true,
			errContains: `whitespace before '=' is not allowed`,
		},
		{
			// 查询不存在的 key 应返回空值且不报错。
			name: "key not found returns empty",
			envContent: `KEY1='foo'
KEY2='bar'
`,
			key:       "KEY3",
			wantValue: ``,
		},
		{
			// 单引号内的 # 是普通字符，不开启注释。
			name: "value with embedded #",
			envContent: `KEY1='foo#notcomment'
KEY2='bar'
`,
			key:       "KEY1",
			wantValue: `foo#notcomment`,
		},
		{
			name: "key with whitespace (should error)",
			envContent: `KEY1 ='foo'
KEY2='bar'
`,
			key:         "KEY1",
			wantErr:     true,
			errContains: "whitespace before '=' is not allowed",
		},
		{
			// 单引号内的首尾空白必须原样保留。
			name: "value with leading and trailing whitespace",
			envContent: `KEY1='   foo bar   '
KEY2='bar'
`,
			key:       "KEY1",
			wantValue: `   foo bar   `,
		},
		{
			name: "multiple comments and blank lines",
			envContent: `# first comment

# second comment
KEY1='foo'

# third comment
KEY2='bar'
`,
			key:       "KEY2",
			wantValue: `bar`,
		},
		{
			// 空字符串是合法值。
			name: "empty value",
			envContent: `KEY1='foo'
KEY2=''
KEY3='bar'
`,
			key:       "KEY2",
			wantValue: ``,
		},
		{
			// 缺少 key 的行应报格式错误。
			name: "key is empty (should error)",
			envContent: `='foo'
KEY2='bar'
`,
			key:         "KEY2",
			wantErr:     true,
			errContains: "invalid environment variable format",
		},
		{
			// 只按第一个 = 分隔，值中其余 = 原样保留。
			name: "multiple = in value",
			envContent: `KEY1='foo=bar=baz'
KEY2='bar'
`,
			key:       "KEY1",
			wantValue: `foo=bar=baz`,
		},
		{
			// 闭合引号后的注释允许存在。
			name: "comment after key value pair",
			envContent: `KEY1='foo'
KEY2='bar' # comment
`,
			key:       "KEY2",
			wantValue: `bar`,
		},
		{
			// 错误信息需指出所在行号，便于定位。
			name: "missing key triggers error with line number",
			envContent: `='foo'
KEY2='bar'`,
			key:         "KEY2",
			wantErr:     true,
			errContains: "at line 1",
		},
		{
			// 单引号内不做 shell 变量展开。
			name: "value contains $VAR, should not expand",
			envContent: `KEY1='$VAR'
KEY2='bar'`,
			key:       "KEY1",
			wantValue: `$VAR`,
		},
		{
			// 花括号形式同样不展开。
			name: "value contains ${VAR}, should not expand",
			envContent: `KEY1='${VAR}'
KEY2='bar'`,
			key:       "KEY1",
			wantValue: `${VAR}`,
		},
		{
			// 环境变量名也不展开。
			name: "value contains $HOME, should not expand",
			envContent: `KEY1='$HOME'
KEY2='bar'`,
			key:       "KEY1",
			wantValue: `$HOME`,
		},
		{
			// 混合的 shell 变量语法整体按字面量保留。
			name: "value contains mixed shell variable syntax, should not expand",
			envContent: `KEY1='foo$BAR-${HOME}_$'
KEY2='bar'`,
			key:       "KEY1",
			wantValue: `foo$BAR-${HOME}_$`,
		},
		{
			// 无法识别的行必须报错并标注行号。
			name: "invalid line triggers error with line number",
			envContent: `KEY1='foo'
INVALID_LINE
KEY2='bar'
`,
			key:         "KEY2",
			wantErr:     true,
			errContains: "at line 2",
		},
		{
			// 值必须由单引号包裹。
			name: "unquoted value triggers error",
			envContent: `KEY1='foo'
KEY2=bar
KEY3='baz'
`,
			key:         "KEY2",
			wantErr:     true,
			errContains: "value must be enclosed in single quotes",
		},
		{
			// 双引号不被接受，避免隐式转义语义。
			name: "double quoted value triggers error",
			envContent: `KEY1='foo'
KEY2="bar"
KEY3='baz'
`,
			key:         "KEY2",
			wantErr:     true,
			errContains: "value must be enclosed in single quotes",
		},
		{
			// 多个相邻的单引号串不允许拼接。
			name:        "value with multiple adjacent quoted strings",
			envContent:  `KEY1='foo''bar'`,
			key:         "KEY1",
			wantErr:     true,
			errContains: `unexpected content after closing quote`,
		},
		{
			// 未闭合的单引号应报错。
			name: "unclosed single quote triggers error",
			envContent: `KEY1='foo'
KEY2='bar
KEY3='baz'
`,
			key:         "KEY2",
			wantErr:     true,
			errContains: "unexpected content after closing quote",
		},
		{
			// 闭合引号后只能跟空白或注释。
			name: "content after closing quote triggers error",
			envContent: `KEY1='foo'
KEY2='bar'baz
KEY3='baz'
`,
			key:         "KEY2",
			wantErr:     true,
			errContains: "unexpected content after closing quote",
		},
		{
			// 空引号表示空值。
			name: "empty quotes preserve literal content",
			envContent: `KEY1=''
KEY2='   '
KEY3='bar'
`,
			key:       "KEY1",
			wantValue: ``,
		},
		{
			// 引号内的空格与内容完全保留。
			name: "quotes preserve all literal content including spaces",
			envContent: `KEY1='  foo  bar  '
KEY2='bar'
`,
			key:       "KEY1",
			wantValue: `  foo  bar  `,
		},
		{
			// shell 元字符在单引号内均为字面量。
			name: "special characters in value",
			envContent: `KEY1='!@#$%^&*()_+-=[]{}|;:,.<>?/'
KEY2='bar'
`,
			key:       "KEY1",
			wantValue: `!@#$%^&*()_+-=[]{}|;:,.<>?/`,
		},
		{
			// 这里的 \n、\t 是字面反斜杠转义，不会被解释成换行/制表符。
			name: "newlines and tabs in value",
			envContent: `KEY1='line1\nline2\tline3'
KEY2='bar'
`,
			key:       "KEY1",
			wantValue: `line1\nline2\tline3`,
		},
		{
			// 多语言与 emoji 内容按 UTF-8 原样保留。
			name: "unicode characters in value",
			envContent: `KEY1='中文 Español Français 🌍'
KEY2='bar'
`,
			key:       "KEY1",
			wantValue: `中文 Español Français 🌍`,
		},
		{
			// 反斜杠不做转义处理。
			name: "backslashes in value",
			envContent: `KEY1='path\\to\\file'
KEY2='bar'
`,
			key:       "KEY1",
			wantValue: `path\\to\\file`,
		},
		{
			// 单引号无法在单引号串内转义，因此应报错。
			name: "single quotes within value quotes",
			envContent: `KEY1='value with \'nested\' quotes'
KEY2='bar'
`,
			key:         "KEY1",
			wantErr:     true,
			errContains: "unexpected content after closing quote",
		},
		{
			// 单引号串内的双引号是普通字符。
			name: "double quotes within value quotes",
			envContent: `KEY1='value with "nested" quotes'
KEY2='bar'
`,
			key:       "KEY1",
			wantErr:   false,
			wantValue: `value with "nested" quotes`,
		},
		{
			name: "empty single quotes",
			envContent: `KEY1=''
KEY2=''
KEY3='bar'
`,
			key:       "KEY2",
			wantValue: ``,
		},
		{
			// 仅含空白的值也应保留。
			name: "only whitespace in quotes",
			envContent: `KEY1='   '
KEY2='\t\n'
KEY3='bar'
`,
			key:       "KEY1",
			wantValue: "   ",
		},
		{
			// JSON 结构的引号与标点不应被特殊处理。
			name: "complex JSON-like value",
			envContent: `KEY1='{"name": "test", "value": 123, "nested": {"key": "val"}}'
KEY2='bar'
`,
			key:       "KEY1",
			wantValue: `{"name": "test", "value": 123, "nested": {"key": "val"}}`,
		},
		{
			// URL 中的 ?、&、= 等字符保持字面量。
			name: "URL with special characters",
			envContent: `KEY1='https://example.com/path?query=value&another=param'
KEY2='bar'
`,
			key:       "KEY1",
			wantValue: `https://example.com/path?query=value&another=param`,
		},
		{
			// XML 标签与属性引号原样保留。
			name: "XML-like content",
			envContent: `KEY1='<root><element attr="value">content</element></root>'
KEY2='bar'
`,
			key:       "KEY1",
			wantValue: `<root><element attr="value">content</element></root>`,
		},
		{
			// 模板占位符形式的编码数据原样保留。
			name: "base64 encoded data",
			envContent: `KEY1='SGVsbG8gV29ybGQh'
KEY2='bar'
`,
			key:       "KEY1",
			wantValue: `SGVsbG8gV29ybGQh`,
		},
		{
			// 正则中的反斜杠与特殊符号不被转义。
			name: "regex pattern",
			envContent: `KEY1='^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'
KEY2='bar'
`,
			key:       "KEY1",
			wantValue: `^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$`,
		},
		{
			// 多行值中的缩进与空格必须逐行保留。
			name: "multi-line value with spaces",
			envContent: `KEY1='line one
  line two with spaces
    line three with more spaces'
KEY2='bar'
`,
			key: "KEY1",
			wantValue: `line one
  line two with spaces
    line three with more spaces`,
		},
		{
			// 多行值中的特殊字符同样不做处理。
			name: "multi-line value with special characters",
			envContent: `KEY1='line1: !@#$%^&*()
line2: []{}|;:,.<>?/
line3: \\\\tabs\\nnewlines'
KEY2='bar'
`,
			key: "KEY1",
			wantValue: `line1: !@#$%^&*()
line2: []{}|;:,.<>?/
line3: \\\\tabs\\nnewlines`,
		},
		{
			// 多行混合内容（变量引用、空白、特殊字符）整体保留。
			name: "multi-line value with mixed content",
			envContent: `KEY1='First line
Second line with $VAR and ${HOME}
Third line with spaces    and    tabs\\t
Fourth line with special chars: !@#$%^&*()_+'
KEY2='bar'
`,
			key: "KEY1",
			wantValue: `First line
Second line with $VAR and ${HOME}
Third line with spaces    and    tabs\\t
Fourth line with special chars: !@#$%^&*()_+`,
		},
		{
			// 多行值中的空行不能被丢弃。
			name: "multi-line value with empty lines",
			envContent: `KEY1='line1

line3 after empty line

line5 after another empty'
KEY2='bar'
`,
			key: "KEY1",
			wantValue: `line1

line3 after empty line

line5 after another empty`,
		},
		{
			// 每行末尾空白也必须保留。
			name: "multi-line value with trailing spaces",
			envContent: `KEY1='line1
line2 with trailing spaces
line3   '
KEY2='bar'
`,
			key: "KEY1",
			wantValue: `line1
line2 with trailing spaces
line3   `,
		},
		{
			// 多行 Unicode 内容逐字节保留。
			name: "multi-line value with unicode characters",
			envContent: `KEY1='中文 第一行
English second line
Français troisième ligne 🌍
Русский четвертая строка'
KEY2='bar'
`,
			key: "KEY1",
			wantValue: `中文 第一行
English second line
Français troisième ligne 🌍
Русский четвертая строка`,
		},
		{
			// 代码片段中的引号与反斜杠按字面量处理。
			name: "multi-line value with code-like content",
			envContent: `KEY1='func main() {
    fmt.Println(\"Hello, World!\")
    for i := 0; i < 10; i++ {
        fmt.Printf(\"i=%d\\n\", i)
    }
}'
KEY2='bar'
`,
			key: "KEY1",
			wantValue: `func main() {
    fmt.Println(\"Hello, World!\")
    for i := 0; i < 10; i++ {
        fmt.Printf(\"i=%d\\n\", i)
    }
}`,
		},
		{
			// 多行 JSON 结构完整保留。
			name: "multi-line value with JSON content",
			envContent: `KEY1='{
  "name": "test",
  "value": 123,
  "nested": {
    "key": "val",
    "array": [1, 2, 3]
  }
}'
KEY2='bar'
`,
			key: "KEY1",
			wantValue: `{
  "name": "test",
  "value": 123,
  "nested": {
    "key": "val",
    "array": [1, 2, 3]
  }
}`,
		},
		{
			// 多行 YAML 结构完整保留。
			name: "multi-line value with YAML content",
			envContent: `KEY1='name: test
value: 123
nested:
  key: val
  array:
    - 1
    - 2
    - 3'
KEY2='bar'
`,
			key: "KEY1",
			wantValue: `name: test
value: 123
nested:
  key: val
  array:
    - 1
    - 2
    - 3`,
		},
		{
			// 多行证书类占位内容应原样保留。
			name: "multi-line value with Cert",
			envContent: `KEY1='-----BEGIN OPENSSH PRIVATE KEY-----
b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQSSSSSSSSSSSSSBlwAAAAdzc2gtcn
NhAAAAAwEAAQAAAYEAyUgx65kBQf/Nl+EcOFkP8q7n7FDetNrH7WYAnpYnaWG8w4rA9ht9
upttTNyRbCVpb32f5m8DFvVug0vAa/ksX8iSKzD53bWTuTQiN/i9Q/iG0eCNPR0KoW/gLV
SS9pIWWVYNav6dBjknR85202+YryJkn8THAf8Kg5Lwl0dom41dZvN1DirbcqWOU1BKG2sl
pFIq8BdXedRjwoNYngMvLBOb4CAfvQyKr1+ARQecqzPn4pTovYtIZRasIgrBOpSpHGZa70
pTAVP5+KGXN5DigjQUWaje1AiEx5zk8J3T5AA0abSaNn0uE53tjgalTzcY9kxHdo2rgHJC
huHhWsWslkcntkKp0V1Jc8oGv86Dp5mPhpfpMOK+vCe2TrS/saes9fNVxjorSpLl4xTU/V
-----END OPENSSH PRIVATE KEY-----'
KEY2='bar'
`,
			key: "KEY1",
			wantValue: `-----BEGIN OPENSSH PRIVATE KEY-----
b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQSSSSSSSSSSSSSBlwAAAAdzc2gtcn
NhAAAAAwEAAQAAAYEAyUgx65kBQf/Nl+EcOFkP8q7n7FDetNrH7WYAnpYnaWG8w4rA9ht9
upttTNyRbCVpb32f5m8DFvVug0vAa/ksX8iSKzD53bWTuTQiN/i9Q/iG0eCNPR0KoW/gLV
SS9pIWWVYNav6dBjknR85202+YryJkn8THAf8Kg5Lwl0dom41dZvN1DirbcqWOU1BKG2sl
pFIq8BdXedRjwoNYngMvLBOb4CAfvQyKr1+ARQecqzPn4pTovYtIZRasIgrBOpSpHGZa70
pTAVP5+KGXN5DigjQUWaje1AiEx5zk8J3T5AA0abSaNn0uE53tjgalTzcY9kxHdo2rgHJC
huHhWsWslkcntkKp0V1Jc8oGv86Dp5mPhpfpMOK+vCe2TrS/saes9fNVxjorSpLl4xTU/V
-----END OPENSSH PRIVATE KEY-----`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 每个用例写入独立临时文件，ParseEnv 只接受文件路径。
			tmpFile, err := os.CreateTemp(tempDir, "envtest_*")
			if err != nil {
				t.Fatalf("failed to create temp file: %v", err)
			}
			defer func() { _ = os.Remove(tmpFile.Name()) }()

			if _, err := tmpFile.Write([]byte(tt.envContent)); err != nil {
				t.Fatalf("failed to write to temp file: %v", err)
			}
			if err := tmpFile.Close(); err != nil {
				t.Fatalf("failed to close temp file: %v", err)
			}
			// 解析结果与期望值比较；出错用例只校验错误信息。
			gotValue, err := ParseEnv(tmpFile.Name(), tt.key)
			// 出错用例允许错误信息不同，但必须包含指定片段。
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error, got none")
					return
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error = %v, want error containing %q", err, tt.errContains)
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if gotValue != tt.wantValue {
				t.Errorf("got %q, want %q", gotValue, tt.wantValue)
			}

			// 非 Windows 平台上再与 bash 的解析结果对比，确认语义一致。
			if runtime.GOOS != "windows" {
				shellValue, shellErr := testShellBehavior(t, tt.envContent, tt.key)
				if shellErr != nil {
					t.Errorf("shell failed to parse valid syntax: %v", shellErr)
					return
				}
				if gotValue != shellValue {
					t.Errorf("shell behavior mismatch: ParseEnv=%q, shell=%q", gotValue, shellValue)
				}
			}
		})
	}
}
