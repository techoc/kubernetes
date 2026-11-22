/*
Copyright 2015 The Kubernetes Authors.

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
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestGetEnvAsStringOrFallback 验证环境变量已设置时返回值，
// 未设置时返回 fallback，且 fallback 本身不再被解析。
func TestGetEnvAsStringOrFallback(t *testing.T) {
	const expected = "foo"

	assert := assert.New(t)

	key := "FLOCKER_SET_VAR"
	t.Setenv(key, expected)
	assert.Equal(expected, GetEnvAsStringOrFallback(key, "~"+expected))

	key = "FLOCKER_UNSET_VAR"
	assert.Equal(expected, GetEnvAsStringOrFallback(key, expected))
}

// TestGetEnvAsIntOrFallback 覆盖整数环境变量已设置、未设置和值非法三种情况；
// 值非法时返回 fallback 并携带解析错误。
func TestGetEnvAsIntOrFallback(t *testing.T) {
	const expected = 1

	assert := assert.New(t)

	key := "FLOCKER_SET_VAR"
	t.Setenv(key, strconv.Itoa(expected))
	returnVal, _ := GetEnvAsIntOrFallback(key, 1)
	assert.Equal(expected, returnVal)

	key = "FLOCKER_UNSET_VAR"
	returnVal, _ = GetEnvAsIntOrFallback(key, expected)
	assert.Equal(expected, returnVal)

	key = "FLOCKER_SET_VAR"
	t.Setenv(key, "not-an-int")
	returnVal, err := GetEnvAsIntOrFallback(key, 1)
	assert.Equal(expected, returnVal)
	if err == nil {
		t.Error("expected error")
	}
}

// TestGetEnvAsFloat64OrFallback 覆盖浮点环境变量已设置、未设置和值非法三种情况，
// 并断言非法值产生的具体解析错误。
func TestGetEnvAsFloat64OrFallback(t *testing.T) {
	const expected = 1.0

	assert := assert.New(t)

	key := "FLOCKER_SET_VAR"
	t.Setenv(key, "1.0")
	returnVal, _ := GetEnvAsFloat64OrFallback(key, 2.0)
	assert.Equal(expected, returnVal)

	key = "FLOCKER_UNSET_VAR"
	returnVal, _ = GetEnvAsFloat64OrFallback(key, 1.0)
	assert.Equal(expected, returnVal)

	key = "FLOCKER_SET_VAR"
	t.Setenv(key, "not-a-float")
	returnVal, err := GetEnvAsFloat64OrFallback(key, 1.0)
	assert.Equal(expected, returnVal)
	assert.EqualError(err, "strconv.ParseFloat: parsing \"not-a-float\": invalid syntax")
}
