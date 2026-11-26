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
)

// TestCloudControllerManagerDefaultsRoundTrip 验证默认化后的配置经 JSON 序列化/反序列化
// 不丢失字段。组件配置以 ConfigMap 形式传递，任何不可往返的字段都会在运行时被静默丢弃。
func TestCloudControllerManagerDefaultsRoundTrip(t *testing.T) {
	ks1 := &CloudControllerManagerConfiguration{}
	SetDefaults_CloudControllerManagerConfiguration(ks1)
	cm, err := convertObjToConfigMap("CloudControllerManagerConfiguration", ks1)
	if err != nil {
		t.Errorf("unexpected ConvertObjToConfigMap error %v", err)
	}

	ks2 := &CloudControllerManagerConfiguration{}
	if err = json.Unmarshal([]byte(cm.Data["CloudControllerManagerConfiguration"]), ks2); err != nil {
		t.Errorf("unexpected error unserializing cloud controller manager config %v", err)
	}

	// 用 DeepEqual 比较整个结构，确保默认化没有写入无法序列化的临时状态。
	if !reflect.DeepEqual(ks2, ks1) {
		t.Errorf("Expected:\n%#v\n\nGot:\n%#v", ks1, ks2)
	}
}

// convertObjToConfigMap 把对象转成 ConfigMap，专门用于 ComponentConfig。
// 用 ConfigMap 模拟组件配置的真实载体，便于验证 JSON 编码路径。
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
