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

package v1alpha1

import (
	"encoding/json"
	"reflect"
	"testing"

	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubectrlmgrconfigv1alpha1 "k8s.io/kube-controller-manager/config/v1alpha1"
)

// TestKubeControllerDefaultsRoundTrip 验证默认配置经 ConfigMap 序列化往返后保持不变。
// 控制器配置以 JSON 存入 ConfigMap，任何字段在往返中丢失或改变都会导致
// 启动时加载到的配置与预设默认值不一致。
func TestKubeControllerDefaultsRoundTrip(t *testing.T) {
	ks1 := &kubectrlmgrconfigv1alpha1.KubeControllerManagerConfiguration{}
	// 先填充默认值，再检验序列化往返是否保真。
	SetDefaults_KubeControllerManagerConfiguration(ks1)
	cm, err := convertObjToConfigMap("KubeControllerManagerConfiguration", ks1)
	if err != nil {
		t.Errorf("unexpected ConvertObjToConfigMap error %v", err)
	}

	ks2 := &kubectrlmgrconfigv1alpha1.KubeControllerManagerConfiguration{}
	// 从 ConfigMap 的数据字段反序列化，模拟控制器读取配置的过程。
	if err = json.Unmarshal([]byte(cm.Data["KubeControllerManagerConfiguration"]), ks2); err != nil {
		t.Errorf("unexpected error unserializing controller manager config %v", err)
	}

	if !reflect.DeepEqual(ks2, ks1) {
		t.Errorf("Expected:\n%#v\n\nGot:\n%#v", ks1, ks2)
	}
}

// convertObjToConfigMap 把对象序列化为 ConfigMap 的 data 字段。
// 专用于 ComponentConfig：键与值都使用配置类型名，便于按名称取回。
func convertObjToConfigMap(name string, obj runtime.Object) (*v1.ConfigMap, error) {
	eJSONBytes, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}
	cm := &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Data: map[string]string{
			name: string(eJSONBytes[:]),
		},
	}
	return cm, nil
}
