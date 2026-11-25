/*
Copyright 2022 The Kubernetes Authors.

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

package v2

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/openapi/openapitest"
)

var appsv1Path = "apis/apps/v1"

var appsDeploymentGVR = schema.GroupVersionResource{
	Group:    "apps",
	Version:  "v1",
	Resource: "deployments",
}

// 演示当尝试使用无效的输出名称执行 `Render“ 时，generic 会抛出错误
// 而如果该名称随后被添加为模板，则执行 `Render` 时不会抛出错误
func TestGeneratorMissingOutput(t *testing.T) {
	var buf bytes.Buffer
	var doc map[string]interface{}

	appsv1Bytes := bytesForGV(t, appsv1Path)
	err := json.Unmarshal(appsv1Bytes, &doc)
	require.NoError(t, err)

	gen := NewGenerator()
	badTemplateName := "bad-template"
	err = gen.Render(badTemplateName, doc, appsDeploymentGVR, nil, false, 0, &buf)

	require.ErrorContains(t, err, "unrecognized format: "+badTemplateName)
	require.Zero(t, buf.Len())

	err = gen.AddTemplate(badTemplateName, "ok")
	require.NoError(t, err)

	err = gen.Render(badTemplateName, doc, appsDeploymentGVR, nil, false, 0, &buf)
	require.NoError(t, err)
	require.Equal(t, "ok", buf.String())
}

// 演示包含所传入对象的正确上下文会被传递给模板
func TestGeneratorContext(t *testing.T) {
	var buf bytes.Buffer
	var doc map[string]interface{}

	appsv1Bytes := bytesForGV(t, appsv1Path)
	err := json.Unmarshal(appsv1Bytes, &doc)
	require.NoError(t, err)

	gen := NewGenerator()
	err = gen.AddTemplate("Context", "{{ toJson . }}")
	require.NoError(t, err)

	expectedContext := TemplateContext{
		Document:  doc,
		GVR:       appsDeploymentGVR,
		Recursive: false,
		FieldPath: nil,
	}

	err = gen.Render("Context",
		expectedContext.Document,
		expectedContext.GVR,
		expectedContext.FieldPath,
		expectedContext.Recursive,
		expectedContext.MaxDepth,
		&buf)
	require.NoError(t, err)

	var actualContext TemplateContext
	err = json.Unmarshal(buf.Bytes(), &actualContext)
	require.NoError(t, err)
	require.Equal(t, expectedContext, actualContext)
}

// bytesForGV 以字节切片的形式返回所传入
// group/version 的 OpenAPI V3 规范。假定字节为 json
// 格式。传入的路径字符串形如：
//
//	apis/apps/v1
func bytesForGV(t *testing.T, gvPath string) []byte {
	fakeClient := openapitest.NewEmbeddedFileClient()
	paths, err := fakeClient.Paths()
	require.NoError(t, err)
	gv, found := paths[gvPath]
	require.True(t, found)
	gvBytes, err := gv.Schema("application/json")
	require.NoError(t, err)
	return gvBytes
}
