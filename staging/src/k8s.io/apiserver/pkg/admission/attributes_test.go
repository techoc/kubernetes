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

package admission

import (
	"testing"

	"github.com/stretchr/testify/assert"
	auditinternal "k8s.io/apiserver/pkg/apis/audit"
)

// TestAddAnnotation 覆盖准入注解的写入契约：注解键必须形如 <plugin 域名>/<key>，
// 同一键不可被覆盖，非法键被拒绝且不留下痕迹。
//
// 注解最终会随审计事件输出（getAnnotations(LevelMetadata)），因此写入校验
// 同时是防止插件互相篡改审计记录的边界。
func TestAddAnnotation(t *testing.T) {
	attr := &attributesRecord{}

	// test AddAnnotation
	attr.AddAnnotation("foo.admission.k8s.io/key1", "value1")
	attr.AddAnnotation("foo.admission.k8s.io/key2", "value2")
	annotations := attr.getAnnotations(auditinternal.LevelMetadata)
	assert.Equal(t, "value1", annotations["foo.admission.k8s.io/key1"])

	// test overwrite
	assert.Error(t, attr.AddAnnotation("foo.admission.k8s.io/key1", "value1-overwrite"),
		"admission annotations should not be allowd to be overwritten")
	annotations = attr.getAnnotations(auditinternal.LevelMetadata)
	assert.Equal(t, "value1", annotations["foo.admission.k8s.io/key1"], "admission annotations should not be overwritten")

	// test invalid plugin names
	//
	// 非法输入覆盖四类错误：DNS 子域名格式错误、缺少插件名、缺少 key、空串。
	var testCases = map[string]string{
		"invalid dns subdomain": "INVALID-DNS-Subdomain/policy",
		"no plugin name":        "policy",
		"no key name":           "foo.admission.k8s.io",
		"empty key":             "",
	}
	for name, invalidKey := range testCases {
		err := attr.AddAnnotation(invalidKey, "value-foo")
		assert.Error(t, err)
		annotations = attr.getAnnotations(auditinternal.LevelMetadata)
		assert.Equal(t, "", annotations[invalidKey], name+": invalid pluginName is not allowed ")
	}

	// test all saved annotations
	//
	// 最终只应留下两条合法注解：失败的写入不能产生残留。
	assert.Equal(
		t,
		map[string]string{
			"foo.admission.k8s.io/key1": "value1",
			"foo.admission.k8s.io/key2": "value2",
		}, annotations,
		"unexpected final annotations",
	)
}
