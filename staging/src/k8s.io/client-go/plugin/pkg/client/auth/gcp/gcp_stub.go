/*
Copyright 2022 The Kubernetes Authors.

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

// Package gcp 是占位包，仅保留 "gcp" 认证提供者名称的注册。
//
// Package gcp 只是一个占位实现：GCP 认证插件已被移除，但为了让包含
// auth-provider: gcp 的旧 kubeconfig 得到明确、可操作的错误提示，
// 仍以该名字注册一个始终报错的 provider。
package gcp

import (
	"errors"

	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
)

func init() {
	// 注册失败意味着存在另一个同名 provider，属于编程错误，直接终止进程。
	if err := rest.RegisterAuthProviderPlugin("gcp", newGCPAuthProvider); err != nil {
		klog.Fatalf("Failed to register gcp auth plugin: %v", err)
	}
}

// newGCPAuthProvider 始终返回错误，引导用户迁移到 gke-gcloud-auth-plugin
// 凭证插件；不使用旧的 in-tree auth provider 实现。
func newGCPAuthProvider(_ string, _ map[string]string, _ rest.AuthProviderConfigPersister) (rest.AuthProvider, error) {
	return nil, errors.New(`The gcp auth plugin has been removed.
Please use the "gke-gcloud-auth-plugin" kubectl/client-go credential plugin instead.
See https://cloud.google.com/blog/products/containers-kubernetes/kubectl-auth-changes-in-gke for further details`)
}
