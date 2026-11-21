/*
Copyright 2017 The Kubernetes Authors.

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

package lifecycle

import (
	"context"

	v1 "k8s.io/api/core/v1"
)

// AdmissionFailureHandlerStub 是不对准入失败做任何处理的 AdmissionFailureHandler，
// 只是把失败原样传递下去。
type AdmissionFailureHandlerStub struct{}

var _ AdmissionFailureHandler = &AdmissionFailureHandlerStub{}

// NewAdmissionFailureHandlerStub 返回 AdmissionFailureHandlerStub 实例。
func NewAdmissionFailureHandlerStub() *AdmissionFailureHandlerStub {
	return &AdmissionFailureHandlerStub{}
}

// HandleAdmissionFailure 直接返回传入的失败原因，不做特殊处理。
func (n *AdmissionFailureHandlerStub) HandleAdmissionFailure(ctx context.Context, admitPod *v1.Pod, failureReasons []PredicateFailureReason, operation Operation) ([]PredicateFailureReason, error) {
	return failureReasons, nil
}
