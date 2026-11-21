/*
Copyright 2024 The Kubernetes Authors.

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

package plugin

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"
	drahealthv1 "k8s.io/kubelet/pkg/apis/dra-health/v1"
	drahealthv1alpha1 "k8s.io/kubelet/pkg/apis/dra-health/v1alpha1"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// TestPickHealthService 验证在多个健康服务版本中选择 kubelet 支持的版本。
func TestPickHealthService(t *testing.T) {
	for name, tc := range map[string]struct {
		supportedServices []string
		want              string
	}{
		"none": {
			supportedServices: []string{"v1beta1.DRAPlugin"},
			want:              "",
		},
		"empty": {
			supportedServices: nil,
			want:              "",
		},
		// v1 之前发布的驱动只提供 v1alpha1。
		// kubelet 会在三个版本的过渡期内继续使用它，
		// 参见 healthServicesSupportedByKubelet。
		"only-v1alpha1": {
			supportedServices: []string{"v1beta1.DRAPlugin", drahealthv1alpha1.DRAResourceHealthService},
			want:              drahealthv1alpha1.DRAResourceHealthService,
		},
		"only-v1": {
			supportedServices: []string{"v1beta1.DRAPlugin", drahealthv1.DRAResourceHealthService},
			want:              drahealthv1.DRAResourceHealthService,
		},
		"both-picks-v1": {
			supportedServices: []string{drahealthv1alpha1.DRAResourceHealthService, drahealthv1.DRAResourceHealthService},
			want:              drahealthv1.DRAResourceHealthService,
		},
	} {
		t.Run(name, func(t *testing.T) {
			if got := pickHealthService(tc.supportedServices); got != tc.want {
				t.Errorf("pickHealthService(%v) = %q, want %q", tc.supportedServices, got, tc.want)
			}
		})
	}
}

// TestAddSameName 验证同名插件的添加、替换与删除行为。
func TestAddSameName(t *testing.T) {
	tCtx := ktesting.Init(t)
	// 随机驱动名用于避免与其他测试冲突。
	driverName := fmt.Sprintf("dummy-driver-%d", rand.IntN(10000))

	// 确保测试使用的插件已注册。
	draPlugins := NewDRAPluginManager(tCtx, nil, nil, nil, 0)
	tCtx.ExpectNoError(draPlugins.add(driverName, "old.sock", "", "", defaultClientCallTimeout), "add first plugin")
	p, err := draPlugins.GetPlugin(driverName)
	tCtx.ExpectNoError(err, "get first plugin")

	// 同名且同 endpoint 的重复添加应报错。
	require.Error(tCtx, draPlugins.add(driverName, "old.sock", "", "", defaultClientCallTimeout))

	tCtx.ExpectNoError(draPlugins.add(driverName, "new.sock", "", "", defaultClientCallTimeout), "add second plugin")
	p2, err := draPlugins.GetPlugin(driverName)
	tCtx.ExpectNoError(err, "get second plugin")
	if p == p2 {
		tCtx.Fatal("expected to get second plugin, got first one again")
	}

	// 删除旧插件。
	draPlugins.remove(p.driverName, p.endpoint)
	plugin, err := draPlugins.GetPlugin(driverName)

	// 此时应只剩下新插件 p2。
	tCtx.ExpectNoError(err, "get plugin")
	if p2 != plugin {
		tCtx.Fatal("expected to get second plugin again, got something else")
	}
}

// TestDelete 验证删除插件后无法再查询到它。
func TestDelete(t *testing.T) {
	tCtx := ktesting.Init(t)
	driverName := fmt.Sprintf("dummy-driver-%d", rand.IntN(10000))
	socketFile := "dra.sock"

	// 确保测试使用的插件已注册。
	draPlugins := NewDRAPluginManager(tCtx, nil, nil, &mockStreamHandler{}, 0)
	tCtx.ExpectNoError(draPlugins.add(driverName, "dra.sock", "", "", defaultClientCallTimeout), "add plugin")

	draPlugins.remove(driverName, socketFile)

	_, err := draPlugins.GetPlugin(driverName)
	require.Error(t, err, "plugin should not exist after being removed")
}
