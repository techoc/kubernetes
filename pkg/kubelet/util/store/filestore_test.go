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

package store

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/kubernetes/pkg/util/filesystem"
)

// TestFileStore 验证文件存储的基本读写、列举与删除行为。
func TestFileStore(t *testing.T) {
	store, err := NewFileStore("/FileStore", filesystem.NewTempFs())
	assert.NoError(t, err)
	testStore(t, store)
}

// testStore 对给定 Store 实现执行一组通用的一致性测试。
func testStore(t *testing.T, store Store) {
	// 覆盖合法 key 与包含路径分隔符、前导点、空白和非法字符的 key。
	testCases := []struct {
		key       string
		data      string
		expectErr bool
	}{
		{
			"id1",
			"data1",
			false,
		},
		{
			"id2",
			"data2",
			false,
		},
		{
			"/id1",
			"data1",
			true,
		},
		{
			".id1",
			"data1",
			true,
		},
		{
			"   ",
			"data2",
			true,
		},
		{
			"___",
			"data2",
			true,
		},
	}

	// 测试写入数据。
	// 逐个用例验证写入前读取失败，写入结果符合预期。
	for _, c := range testCases {
		t.Log("test case: ", c)
		_, err := store.Read(c.key)
		assert.Error(t, err)

		err = store.Write(c.key, []byte(c.data))
		if c.expectErr {
			assert.Error(t, err)
			continue
		}

		require.NoError(t, err)
		// 按 key 读回数据并校验。
		data, err := store.Read(c.key)
		require.NoError(t, err)
		assert.Equal(t, string(data), c.data)
	}

	// 测试列出 key。
	keys, err := store.List()
	assert.NoError(t, err)
	sort.Strings(keys)
	assert.Equal(t, []string{"id1", "id2"}, keys)

	// 测试删除数据。
	for _, c := range testCases {
		if c.expectErr {
			continue
		}

		// 删除后再次读取应返回 key 不存在。
		err = store.Delete(c.key)
		require.NoError(t, err)
		_, err = store.Read(c.key)
		assert.EqualValues(t, ErrKeyNotFound, err)
	}

	// 删除不存在的 key 应成功。
	err = store.Delete("id1")
	assert.NoError(t, err)

	// 测试列出 key。
	keys, err = store.List()
	require.NoError(t, err)
	assert.Empty(t, keys)
}
