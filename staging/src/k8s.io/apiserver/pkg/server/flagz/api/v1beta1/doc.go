/*
Copyright The Kubernetes Authors.

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

// +k8s:deepcopy-gen=package
// +k8s:openapi-gen=true
// +k8s:openapi-model-package=io.k8s.apiserver.pkg.server.flagz.api.v1beta1

// Package v1beta1 contains API Schema definitions for the flagz v1beta1 API group
//
// flagz 暴露 apiserver 当前生效的命令行参数，便于运维排查配置漂移。
package v1beta1
