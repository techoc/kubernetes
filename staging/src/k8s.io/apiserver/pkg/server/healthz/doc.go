/*
Copyright 2014 The Kubernetes Authors.

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

// Package healthz implements basic http server health checking.
// Usage:
//
//	import "k8s.io/apiserver/pkg/server/healthz"
//	healthz.InstallHandler(mux)
//
// 检查项分为 liveness（/livez）与 readiness（/readyz）：前者失败通常意味着
// 需要重启进程，后者失败只表示暂时不应接收流量，两者不可混用。
package healthz
