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

package main

import (
	"os"
)

// Getenver 是我们为方便测试而用于模拟环境的接口。OS 环境变量
// 不容易测试，因为其内部使用了 sync.Once。
type Getenver interface {
	Getenv(string) string
}

// osEnv 使用真正的 os.Getenv 方法查找值。
type osEnv struct{}

// Getenv 获取所请求环境变量的值。
func (*osEnv) Getenv(s string) string {
	return os.Getenv(s)
}

// explicitEnv 使用 map 代替 os.Getenv 方法查找值。
type explicitEnv struct {
	vals map[string]string
}

// Getenv 返回所请求环境变量的值
// （在此实现中实际上就是一次 map 查找）。
func (e *explicitEnv) Getenv(s string) string {
	return e.vals[s]
}

// defaultOSEnv 使用 Getenver 查找值，若找不到该值，
// 则回退到其内部的默认集合。
type defaultEnver struct {
	firstChoice Getenver
	defaults    map[string]string
}

// Getenv 返回环境变量的值；若未设置则返回其默认值。
func (e *defaultEnver) Getenv(s string) string {
	v := e.firstChoice.Getenv(s)
	if len(v) == 0 {
		return e.defaults[s]
	}
	return v
}

func envWithDefaults(defaults map[string]string) Getenver {
	return &defaultEnver{firstChoice: &osEnv{}, defaults: defaults}
}
