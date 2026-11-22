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

// Package register 通过导入副作用注册 JSON 日志格式。
// 组件只需空白导入本包，即可在 --logging-format=json 时获得 JSON 输出；
// 注册由 LoggingBetaOptions gate 控制，因此格式仍属于 Beta 能力。
package register

import (
	logsapi "k8s.io/component-base/logs/api/v1"
	json "k8s.io/component-base/logs/json"
)

func init() {
	// JSON 是 klog 的可选格式；注册失败说明格式名冲突，属于编程错误。
	if err := logsapi.RegisterLogFormat(logsapi.JSONLogFormat, json.Factory{}, logsapi.LoggingBetaOptions); err != nil {
		panic(err)
	}
}
