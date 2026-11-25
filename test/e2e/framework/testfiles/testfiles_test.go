/*
Copyright 2021 The Kubernetes Authors.

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

package testfiles

import (
	"embed"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	fooContents = `Hello World
`
	fooPath = "testdata/a/foo.txt"

	notExistsPath = "testdata/b"

	expectedDescription = `The following files are embedded into the test executable:
	testdata/a/foo.txt`
)

//go:embed testdata/a
var testFS embed.FS

func getTestEmbeddedSource() *EmbeddedFileSource {
	return &EmbeddedFileSource{
		EmbeddedFS: testFS,
	}
}

func TestEmbeddedFileSource(t *testing.T) {
	s := getTestEmbeddedSource()

	// 读取一个存在的文件并比较其内容
	b, err := s.ReadTestFile(fooPath)

	require.NoError(t, err)
	assert.Equal(t, fooContents, string(b))

	// 读取一个不存在的文件，确保返回值为空且 error 为 nil
	// 注意：这样做是为了让调用方可以尝试下一个文件来源
	b, err = s.ReadTestFile(notExistsPath)
	require.NoError(t, err)
	assert.Empty(t, b)

	// 描述测试文件系统时应列出所有文件
	assert.Equal(t, expectedDescription, s.DescribeFiles())
}
