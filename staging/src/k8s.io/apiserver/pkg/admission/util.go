/*
Copyright 2019 The Kubernetes Authors.

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

import "k8s.io/apimachinery/pkg/runtime"

// RuntimeObjectInterfaces 把 runtime.Scheme 提供的各项能力打包成
// admission 插件使用的 ObjectInterfaces。
// 通过内嵌接口而不是持有 *runtime.Scheme，插件只能看到自己确实需要的
// 能力，也便于测试时替换其中任意一项。
type RuntimeObjectInterfaces struct {
	runtime.ObjectCreater
	runtime.ObjectTyper
	runtime.ObjectDefaulter
	runtime.ObjectConvertor
	runtime.EquivalentResourceMapper
}

// NewObjectInterfacesFromScheme 基于 scheme 构造 ObjectInterfaces。
// 前四项能力都委托给同一个 scheme；EquivalentResourceMapper 使用新的注册表，
// 因为等价资源映射由调用方按需填充，不应共享全局状态。
func NewObjectInterfacesFromScheme(scheme *runtime.Scheme) ObjectInterfaces {
	return &RuntimeObjectInterfaces{scheme, scheme, scheme, scheme, runtime.NewEquivalentResourceRegistry()}
}

// GetObjectCreater 返回对象创建器。
func (r *RuntimeObjectInterfaces) GetObjectCreater() runtime.ObjectCreater {
	return r.ObjectCreater
}

// GetObjectTyper 返回类型信息查询器。
func (r *RuntimeObjectInterfaces) GetObjectTyper() runtime.ObjectTyper {
	return r.ObjectTyper
}

// GetObjectDefaulter 返回默认值填充器。
func (r *RuntimeObjectInterfaces) GetObjectDefaulter() runtime.ObjectDefaulter {
	return r.ObjectDefaulter
}

// GetObjectConvertor 返回版本转换器。
func (r *RuntimeObjectInterfaces) GetObjectConvertor() runtime.ObjectConvertor {
	return r.ObjectConvertor
}

// GetEquivalentResourceMapper 返回等价资源映射器。
func (r *RuntimeObjectInterfaces) GetEquivalentResourceMapper() runtime.EquivalentResourceMapper {
	return r.EquivalentResourceMapper
}
