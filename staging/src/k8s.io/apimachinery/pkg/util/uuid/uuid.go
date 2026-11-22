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

// Package uuid 提供生成 Kubernetes UID 的单入口，屏蔽底层 UUID 实现。
package uuid

import (
	"github.com/google/uuid"

	"k8s.io/apimachinery/pkg/types"
)

// NewUUID 返回随机（v4）UUID 并包装为 types.UID。
// 使用 google/uuid 的随机源，确保 UID 不可预测、碰撞概率可忽略。
func NewUUID() types.UID {
	return types.UID(uuid.New().String())
}
