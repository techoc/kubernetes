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

// Package configz 从运行中的组件提供 ComponentConfig 对象。
// 需要提供配置的组件创建一个 Config 对象，程序调用一次 InstallHandler。例如：
//
//	func main() {
//		if err := setupConfigz(); err != nil {
//			klog.Fatalf("Failed to setup configz: %v", err)
//		}
//		http.ListenAndServe(":8080", http.DefaultServeMux)
//	}
//
//	func setupConfigz() error {
//		boatConfig := getBoatConfig()
//		planeConfig := getPlaneConfig()
//
//		bcz, err := configz.New("boat")
//		if err != nil {
//			return fmt.Errorf("unable to register boat config: %w", err)
//		}
//		if err := bcz.Set(boatConfig); err != nil {
//			return fmt.Errorf("unable to set boat config: %w", err)
//		}
//
//		pcz, err := configz.New("plane")
//		if err != nil {
//			return fmt.Errorf("unable to register plane config: %w", err)
//		}
//		if err := pcz.Set(planeConfig); err != nil {
//			return fmt.Errorf("unable to set plane config: %w", err)
//		}
//
//		configz.InstallHandler(http.DefaultServeMux)
//		return nil
//	}
package configz

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"k8s.io/apimachinery/pkg/runtime"
)

const DefaultConfigzPath = "/configz"

var (
	// configsGuard 保护 configs：HTTP handler 读取与组件启动时注册可能并发。
	configsGuard sync.RWMutex
	configs      = map[string]*Config{}
)

// Config 是 ComponentConfig 对象的句柄，不应直接创建，请使用 New()。
type Config struct {
	val runtime.Object
}

// InstallHandler 在给定 mux 上为 "/configz" 端点添加 HTTP 处理函数，以 JSON 格式
// 提供全部已注册 ComponentConfig。可多次调用（例如不同 mux），注册的是同一个全局处理逻辑。
func InstallHandler(m mux) {
	m.Handle(DefaultConfigzPath, http.HandlerFunc(handle))
}

// mux 抽象 http.ServeMux 的最小接口，便于测试。
type mux interface {
	Handle(string, http.Handler)
}

// New 创建具有给定名称的 Config 对象，并注册到本包的 "/configz" 处理函数。
// 名称重复会报错，避免同一组件配置被两处覆盖。
func New(name string) (*Config, error) {
	configsGuard.Lock()
	defer configsGuard.Unlock()
	if _, found := configs[name]; found {
		return nil, fmt.Errorf("register config %q twice", name)
	}
	newConfig := Config{}
	configs[name] = &newConfig
	return &newConfig, nil
}

// Delete 从本包的 "/configz" 处理函数中删除指定名称的 ComponentConfig。
func Delete(name string) {
	configsGuard.Lock()
	defer configsGuard.Unlock()
	delete(configs, name)
}

// Set 设置此 Config 的 ComponentConfig，只接受带外部 GroupVersion 的对象。
// 内部版本没有稳定的 JSON 表示，直接暴露会随实现变化而破坏消费者。
func (v *Config) Set(val runtime.Object) error {
	configsGuard.Lock()
	defer configsGuard.Unlock()
	if val == nil {
		return fmt.Errorf("val may not be nil")
	}
	gvk := val.GetObjectKind().GroupVersionKind()
	if len(gvk.Kind) == 0 {
		return fmt.Errorf("val must specify a kind")
	}
	if gvk.GroupVersion().Empty() {
		return fmt.Errorf("val must specify a group/version")
	}
	if gvk.Version == runtime.APIVersionInternal {
		return fmt.Errorf("val must specify an external version")
	}
	v.val = val
	return nil
}

// MarshalJSON 将 ComponentConfig 序列化为 JSON 数据。
func (v *Config) MarshalJSON() ([]byte, error) {
	return json.Marshal(v.val)
}

// handle 把内部错误转换为 500 响应。
func handle(w http.ResponseWriter, r *http.Request) {
	if err := write(w); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// write 在锁内完成序列化快照，避免长时间持锁阻塞配置更新。
func write(w http.ResponseWriter) error {
	var b []byte
	var err error
	func() {
		configsGuard.RLock()
		defer configsGuard.RUnlock()
		b, err = json.Marshal(configs)
	}()
	if err != nil {
		return fmt.Errorf("error marshaling json: %v", err)
	}
	// nosniff 防止浏览器把配置内容当作其它 MIME 类型执行。
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	_, err = w.Write(b)
	return err
}
