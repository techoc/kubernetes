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
	"context"
	"fmt"
	"sync"
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
	auditinternal "k8s.io/apiserver/pkg/apis/audit"
	"k8s.io/apiserver/pkg/audit"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeHandler 是可控的准入插件替身：返回值与注解都由测试用例指定，
// 便于验证 WithAudit 装饰器只增加审计信息、不改变决策。
type fakeHandler struct {
	// Admit() 的返回值
	admit error
	// Admit() 阶段写入 attributesRecord 的注解
	admitAnnotations map[string]string
	// Validate() 的返回值
	validate error
	// Validate() 阶段写入 attributesRecord 的注解
	validateAnnotations map[string]string
	// Handles() 的返回值
	handles bool
}

var _ Interface = &fakeHandler{}
var _ MutationInterface = &fakeHandler{}
var _ ValidationInterface = &fakeHandler{}

// Admit 写入变异阶段的注解并返回预设错误。
func (h fakeHandler) Admit(ctx context.Context, a Attributes, o ObjectInterfaces) error {
	for k, v := range h.admitAnnotations {
		a.AddAnnotation(k, v)
	}
	return h.admit
}

// Validate 写入校验阶段的注解并返回预设错误。
func (h fakeHandler) Validate(ctx context.Context, a Attributes, o ObjectInterfaces) error {
	for k, v := range h.validateAnnotations {
		a.AddAnnotation(k, v)
	}
	return h.validate
}

// Handles 返回预设的操作处理能力。
func (h fakeHandler) Handles(o Operation) bool {
	return h.handles
}

// attributes 构造一个空属性记录，足够覆盖注解读写路径。
func attributes() Attributes {
	return NewAttributesRecord(nil, nil, schema.GroupVersionKind{}, "", "", schema.GroupVersionResource{}, "", "", nil, false, nil)
}

// TestWithAudit 验证 WithAudit 包装后：返回值与底层 handler 一致，
// 且插件添加的注解被原样转发到审计事件（含变异与校验两个阶段）。
func TestWithAudit(t *testing.T) {
	var testCases = map[string]struct {
		admit               error
		admitAnnotations    map[string]string
		validate            error
		validateAnnotations map[string]string
		handles             bool
	}{
		"not handle": {
			nil,
			nil,
			nil,
			nil,
			false,
		},
		"allow": {
			nil,
			nil,
			nil,
			nil,
			true,
		},
		"allow with annotations": {
			nil,
			map[string]string{
				"plugin.example.com/foo": "bar",
			},
			nil,
			nil,
			true,
		},
		"allow with annotations overwrite": {
			nil,
			map[string]string{
				"plugin.example.com/foo": "bar",
			},
			nil,
			map[string]string{
				"plugin.example.com/foo": "bar",
			},
			true,
		},
		"forbidden error": {
			NewForbidden(attributes(), fmt.Errorf("quota exceeded")),
			nil,
			NewForbidden(attributes(), fmt.Errorf("quota exceeded")),
			nil,
			true,
		},
		"forbidden error with annotations": {
			NewForbidden(attributes(), fmt.Errorf("quota exceeded")),
			nil,
			NewForbidden(attributes(), fmt.Errorf("quota exceeded")),
			map[string]string{
				"plugin.example.com/foo": "bar",
			},
			true,
		},
		"forbidden error with annotations overwrite": {
			NewForbidden(attributes(), fmt.Errorf("quota exceeded")),
			map[string]string{
				"plugin.example.com/foo": "bar",
			},
			NewForbidden(attributes(), fmt.Errorf("quota exceeded")),
			map[string]string{
				"plugin.example.com/foo": "bar",
			},
			true,
		},
	}
	for tcName, tc := range testCases {
		var handler Interface = fakeHandler{tc.admit, tc.admitAnnotations, tc.validate, tc.validateAnnotations, tc.handles}
		// 审计上下文是注解落点；未初始化时 AddAnnotation 无处可写。
		ctx := audit.WithAuditContext(context.Background())
		ac := audit.AuditContextFrom(ctx)
		if err := ac.Init(audit.RequestAuditConfig{Level: auditinternal.LevelMetadata}, nil); err != nil {
			t.Fatal(err)
		}

		auditHandler := WithAudit(handler)
		a := attributes()

		// Handles 必须原样透传，否则装饰器会改变插件被调用的时机。
		assert.Equal(t, handler.Handles(Create), auditHandler.Handles(Create), tcName+": WithAudit decorator should not effect the return value")

		mutator, ok := handler.(MutationInterface)
		require.True(t, ok)
		auditMutator, ok := auditHandler.(MutationInterface)
		require.True(t, ok)
		assert.Equal(t, mutator.Admit(ctx, a, nil), auditMutator.Admit(ctx, a, nil), tcName+": WithAudit decorator should not effect the return value")

		validator, ok := handler.(ValidationInterface)
		require.True(t, ok)
		auditValidator, ok := auditHandler.(ValidationInterface)
		require.True(t, ok)
		assert.Equal(t, validator.Validate(ctx, a, nil), auditValidator.Validate(ctx, a, nil), tcName+": WithAudit decorator should not effect the return value")

		// 校验阶段后写的注解会覆盖变异阶段的同名注解，因此以 validate 侧为准合并期望值。
		annotations := make(map[string]string, len(tc.admitAnnotations)+len(tc.validateAnnotations))
		for k, v := range tc.admitAnnotations {
			annotations[k] = v
		}
		for k, v := range tc.validateAnnotations {
			annotations[k] = v
		}
		if len(annotations) == 0 {
			assert.Nil(t, ac.GetEventAnnotations(), tcName+": unexptected annotations set in audit event")
		} else {
			assert.Equal(t, annotations, ac.GetEventAnnotations(), tcName+": unexptected annotations set in audit event")
		}
	}
}

// TestWithAuditConcurrency 模拟 store.DeleteCollection 这类对同一属性记录
// 并发调用 Admit 的场景，确保审计装饰器在并发下不 panic、不改变返回值。
// 用 -race 运行才能发现真正的数据竞争。
func TestWithAuditConcurrency(t *testing.T) {
	admitAnnotations := map[string]string{
		"plugin.example.com/foo": "foo",
		"plugin.example.com/bar": "bar",
		"plugin.example.com/baz": "baz",
		"plugin.example.com/qux": "qux",
	}
	var handler Interface = fakeHandler{admitAnnotations: admitAnnotations, handles: true}
	ctx := audit.WithAuditContext(context.Background())
	auditHandler := WithAudit(handler)
	a := attributes()

	// 模拟 store.DeleteCollection 的场景
	workers := 2
	wg := &sync.WaitGroup{}
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			mutator, ok := handler.(MutationInterface)
			if !ok {
				t.Error("handler is not an interface of type MutationInterface")
				return
			}
			auditMutator, ok := auditHandler.(MutationInterface)
			if !ok {
				t.Error("handler is not an interface of type MutationInterface")
				return
			}
			assert.Equal(t, mutator.Admit(ctx, a, nil), auditMutator.Admit(ctx, a, nil), "WithAudit decorator should not effect the return value")
		}()
	}
	wg.Wait()
}
