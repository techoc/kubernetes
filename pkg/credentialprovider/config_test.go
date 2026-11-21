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

package credentialprovider

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// TestReadDockerConfigFile 验证从临时目录读取 dockercfg 配置文件。
func TestReadDockerConfigFile(t *testing.T) {
	configJSONFileName := "config.json"
	var fileInfo *os.File

	// 构造 dockercfg JSON 测试数据。
	inputDockerconfigJSONFile := "{ \"auths\": { \"http://foo.example.com\":{\"auth\":\"Zm9vOmJhcgo=\",\"email\":\"foo@example.com\"}}}"

	// 使用临时目录，避免污染真实的 dockercfg。
	preferredPath, err := os.MkdirTemp("", "test_foo_bar_dockerconfigjson_")
	if err != nil {
		t.Fatalf("Creating tmp dir fail: %v", err)
		return
	}
	defer os.RemoveAll(preferredPath)
	absDockerConfigFileLocation, err := filepath.Abs(filepath.Join(preferredPath, configJSONFileName))
	if err != nil {
		t.Fatalf("While trying to canonicalize %s: %v", preferredPath, err)
	}

	if _, err := os.Stat(absDockerConfigFileLocation); os.IsNotExist(err) {
		// 创建测试配置文件。
		fileInfo, err = os.OpenFile(absDockerConfigFileLocation, os.O_CREATE|os.O_RDWR, 0664)
		if err != nil {
			t.Fatalf("While trying to create file %s: %v", absDockerConfigFileLocation, err)
		}
		defer fileInfo.Close()
	}

	// 写入配置内容，供后续读取校验。
	fileInfo.WriteString(inputDockerconfigJSONFile)

	// 保存原路径并在测试后恢复，避免影响其他用例。
	orgPreferredPath := GetPreferredDockercfgPath()
	SetPreferredDockercfgPath(preferredPath)
	defer SetPreferredDockercfgPath(orgPreferredPath)
	if _, err := ReadDockerConfigFile(); err != nil {
		t.Errorf("Getting docker config file fail : %v preferredPath : %q", err, preferredPath)
	}
}

// TestDockerConfigJsonJSONDecode 验证 .dockerconfigjson 格式的反序列化。
func TestDockerConfigJsonJSONDecode(t *testing.T) {
	// 使用假凭据，避免触及真实凭证。
	input := []byte(`{"auths": {"http://foo.example.com":{"username": "foo", "password": "bar", "email": "foo@example.com"}, "http://bar.example.com":{"username": "bar", "password": "baz", "email": "bar@example.com"}}}`)

	expect := DockerConfigJSON{
		Auths: DockerConfig(map[string]DockerConfigEntry{
			"http://foo.example.com": {
				Username: "foo",
				Password: "bar",
				Email:    "foo@example.com",
			},
			"http://bar.example.com": {
				Username: "bar",
				Password: "baz",
				Email:    "bar@example.com",
			},
		}),
	}

	var output DockerConfigJSON
	err := json.Unmarshal(input, &output)
	if err != nil {
		t.Errorf("Received unexpected error: %v", err)
	}

	if !reflect.DeepEqual(expect, output) {
		t.Errorf("Received unexpected output. Expected %#v, got %#v", expect, output)
	}
}

// TestDockerConfigJSONDecode 验证 .dockercfg 格式的反序列化。
func TestDockerConfigJSONDecode(t *testing.T) {
	// 使用假凭据，避免触及真实凭证。
	input := []byte(`{"http://foo.example.com":{"username": "foo", "password": "bar", "email": "foo@example.com"}, "http://bar.example.com":{"username": "bar", "password": "baz", "email": "bar@example.com"}}`)

	expect := DockerConfig(map[string]DockerConfigEntry{
		"http://foo.example.com": {
			Username: "foo",
			Password: "bar",
			Email:    "foo@example.com",
		},
		"http://bar.example.com": {
			Username: "bar",
			Password: "baz",
			Email:    "bar@example.com",
		},
	})

	var output DockerConfig
	err := json.Unmarshal(input, &output)
	if err != nil {
		t.Errorf("Received unexpected error: %v", err)
	}

	if !reflect.DeepEqual(expect, output) {
		t.Errorf("Received unexpected output. Expected %#v, got %#v", expect, output)
	}
}

// TestDockerConfigEntryJSONDecode 验证单条凭据的 JSON 解码及错误处理。
func TestDockerConfigEntryJSONDecode(t *testing.T) {
	tests := []struct {
		input  []byte
		expect DockerConfigEntry
		fail   bool
	}{
		// 普通情况：直接解析各字段。
		{
			// 使用假凭据，避免触及真实凭证。
			input: []byte(`{"username": "foo", "password": "bar", "email": "foo@example.com"}`),
			expect: DockerConfigEntry{
				Username: "foo",
				Password: "bar",
				Email:    "foo@example.com",
			},
			fail: false,
		},

		// auth 字段可解析出用户名和密码。
		{
			input: []byte(`{"auth": "Zm9vOmJhcg==", "email": "foo@example.com"}`),
			expect: DockerConfigEntry{
				Username: "foo",
				Password: "bar",
				Email:    "foo@example.com",
			},
			fail: false,
		},

		// auth 字段优先于用户名和密码。
		{
			// 使用假凭据，避免触及真实凭证。
			input: []byte(`{"username": "foo", "password": "bar", "auth": "cGluZzpwb25n", "email": "foo@example.com"}`),
			expect: DockerConfigEntry{
				Username: "ping",
				Password: "pong",
				Email:    "foo@example.com",
			},
			fail: false,
		},

		// auth 格式非法时应解析失败。
		{
			input: []byte(`{"auth": "pants", "email": "foo@example.com"}`),
			expect: DockerConfigEntry{
				Username: "",
				Password: "",
				Email:    "foo@example.com",
			},
			fail: true,
		},

		// 非法 JSON 应导致解析失败。
		{
			input: []byte(`{"email": false}`),
			expect: DockerConfigEntry{
				Username: "",
				Password: "",
				Email:    "",
			},
			fail: true,
		},
	}

	for i, tt := range tests {
		var output DockerConfigEntry
		err := json.Unmarshal(tt.input, &output)
		if (err != nil) != tt.fail {
			t.Errorf("case %d: expected fail=%t, got err=%v", i, tt.fail, err)
		}

		if !reflect.DeepEqual(tt.expect, output) {
			t.Errorf("case %d: expected output %#v, got %#v", i, tt.expect, output)
		}
	}
}

// TestDecodeDockerConfigFieldAuth 验证 auth 字段的多种 base64 编码与容错行为。
func TestDecodeDockerConfigFieldAuth(t *testing.T) {
	tests := []struct {
		input    string
		username string
		password string
		fail     bool
	}{
		// auth 字段可解析出用户名和密码。
		{
			input:    "Zm9vOmJhcg==",
			username: "foo",
			password: "bar",
		},

		// 与上一例相同，但 base64 填充不完整。
		{
			input:    "Zm9vOmJhcg",
			username: "foo",
			password: "bar",
		},

		// 与上一例相同，但包含换行符。
		{
			input:    "Zm9vOm\nJhcg==\n",
			username: "foo",
			password: "bar",
		},

		// 标准 base64 编码（带填充）。
		{
			input:    base64.StdEncoding.EncodeToString([]byte("foo:bar")),
			username: "foo",
			password: "bar",
		},

		// 原始 base64 编码（不带填充）。
		{
			input:    base64.RawStdEncoding.EncodeToString([]byte("foo:bar")),
			username: "foo",
			password: "bar",
		},

		// 输入由 encodeDockerConfigFieldAuth 生成（标准编码）。
		{
			input:    encodeDockerConfigFieldAuth("foo", "bar"),
			username: "foo",
			password: "bar",
		},

		// base64 合法，但缺少分隔用户名和密码的冒号。
		{
			input: "cGFudHM=",
			fail:  true,
		},

		// 只忽略换行符，尾部空格仍会导致失败。
		{
			input: "Zm9vOmJhcg== ",
			fail:  true,
		},

		// 非法的 base64 数据。
		{
			input: "pants",
			fail:  true,
		},
	}

	for i, tt := range tests {
		username, password, err := decodeDockerConfigFieldAuth(tt.input)
		if (err != nil) != tt.fail {
			t.Errorf("case %d: expected fail=%t, got err=%v", i, tt.fail, err)
		}

		if tt.username != username {
			t.Errorf("case %d: expected username %q, got %q", i, tt.username, username)
		}

		if tt.password != password {
			t.Errorf("case %d: expected password %q, got %q", i, tt.password, password)
		}
	}
}

// TestDockerConfigEntryJSONCompatibleEncode 验证序列化时会生成兼容的 auth 字段。
func TestDockerConfigEntryJSONCompatibleEncode(t *testing.T) {
	tests := []struct {
		input  DockerConfigEntry
		expect []byte
	}{
		// 普通情况：各字段都应参与序列化。
		{
			// 使用假凭据，避免触及真实凭证。
			expect: []byte(`{"username":"foo","password":"bar","email":"foo@example.com","auth":"Zm9vOmJhcg=="}`),
			input: DockerConfigEntry{
				Username: "foo",
				Password: "bar",
				Email:    "foo@example.com",
			},
		},
	}

	for i, tt := range tests {
		actual, err := json.Marshal(tt.input)
		if err != nil {
			t.Errorf("case %d: unexpected error: %v", i, err)
		}

		if string(tt.expect) != string(actual) {
			t.Errorf("case %d: expected %v, got %v", i, string(tt.expect), string(actual))
		}
	}
}

// TestReadDockerConfigFileFromBytes 验证从字节读取 .dockercfg 的成功与失败路径。
func TestReadDockerConfigFileFromBytes(t *testing.T) {
	testCases := []struct {
		id               string
		input            []byte
		expectedCfg      DockerConfig
		errorExpected    bool
		expectedErrorMsg string
	}{
		{
			id:    "valid input, no error expected",
			input: []byte(`{"http://foo.example.com":{"username": "foo", "password": "bar", "email": "foo@example.com"}}`),
			expectedCfg: DockerConfig(map[string]DockerConfigEntry{
				"http://foo.example.com": {
					Username: "foo",
					Password: "bar",
					Email:    "foo@example.com",
				},
			}),
		},
		{
			id:               "invalid input, error expected",
			input:            []byte(`{"http://foo.example.com":{"username": "foo", "password": "bar", "email": "foo@example.com"`),
			errorExpected:    true,
			expectedErrorMsg: "error occurred while trying to unmarshal json",
		},
	}

	for _, tc := range testCases {
		cfg, err := ReadDockerConfigFileFromBytes(tc.input)
		if err != nil && !tc.errorExpected {
			t.Fatalf("Error was not expected: %v", err)
		}
		if err != nil && tc.errorExpected {
			if !reflect.DeepEqual(err.Error(), tc.expectedErrorMsg) {
				t.Fatalf("Expected error message: `%s` got `%s`", tc.expectedErrorMsg, err.Error())
			}
		} else {
			if !reflect.DeepEqual(cfg, tc.expectedCfg) {
				t.Fatalf("expected: %v got %v", tc.expectedCfg, cfg)
			}
		}
	}
}

// TestReadDockerConfigJSONFileFromBytes 验证从字节读取 .dockerconfigjson 的成功与失败路径。
func TestReadDockerConfigJSONFileFromBytes(t *testing.T) {
	testCases := []struct {
		id               string
		input            []byte
		expectedCfg      DockerConfig
		errorExpected    bool
		expectedErrorMsg string
	}{
		{
			id:    "valid input, no error expected",
			input: []byte(`{"auths": {"http://foo.example.com":{"username": "foo", "password": "bar", "email": "foo@example.com"}, "http://bar.example.com":{"username": "bar", "password": "baz", "email": "bar@example.com"}}}`),
			expectedCfg: DockerConfig(map[string]DockerConfigEntry{
				"http://foo.example.com": {
					Username: "foo",
					Password: "bar",
					Email:    "foo@example.com",
				},
				"http://bar.example.com": {
					Username: "bar",
					Password: "baz",
					Email:    "bar@example.com",
				},
			}),
		},
		{
			id:               "invalid input, error expected",
			input:            []byte(`{"auths": {"http://foo.example.com":{"username": "foo", "password": "bar", "email": "foo@example.com"}, "http://bar.example.com":{"username": "bar", "password": "baz", "email": "bar@example.com"`),
			errorExpected:    true,
			expectedErrorMsg: "error occurred while trying to unmarshal json",
		},
	}

	for _, tc := range testCases {
		cfg, err := readDockerConfigJSONFileFromBytes(tc.input)
		if err != nil && !tc.errorExpected {
			t.Fatalf("Error was not expected: %v", err)
		}
		if err != nil && tc.errorExpected {
			if !reflect.DeepEqual(err.Error(), tc.expectedErrorMsg) {
				t.Fatalf("Expected error message: `%s` got `%s`", tc.expectedErrorMsg, err.Error())
			}
		} else {
			if !reflect.DeepEqual(cfg, tc.expectedCfg) {
				t.Fatalf("expected: %v got %v", tc.expectedCfg, cfg)
			}
		}
	}
}
