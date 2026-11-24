/*
Copyright 2023 The Kubernetes Authors.

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

package servicecidr

import (
	"context"
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/kubernetes/pkg/apis/networking"
)

func newServiceCIDR() *networking.ServiceCIDR {
	return &networking.ServiceCIDR{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "servicecidr-test",
			ResourceVersion: "1",
		},
		Spec: networking.ServiceCIDRSpec{
			CIDRs: []string{"10.10.0.0/24"},
		},
	}
}

// TestServiceCIDRStrategy 验证 CIDR 规范、Feature Gate 对 Status 字段清理的影响及地址范围更新约束。
func TestServiceCIDRStrategy(t *testing.T) {
	if Strategy.NamespaceScoped() {
		t.Errorf("Expected ServiceCIDR to be cluster-scoped")
	}

	obj := &networking.ServiceCIDR{Spec: networking.ServiceCIDRSpec{CIDRs: []string{"bad cidr"}}}

	errors := Strategy.Validate(context.TODO(), obj)
	if len(errors) != 2 {
		t.Errorf("Expected 2 validation errors for invalid object, got %d : %v", len(errors), errors)
	}

	oldObj := newServiceCIDR()
	newObj := oldObj.DeepCopy()
	newObj.Spec.CIDRs = []string{"bad cidr"}
	errors = Strategy.ValidateUpdate(context.TODO(), newObj, oldObj)
	if len(errors) != 1 {
		t.Errorf("Expected 1 validation error for invalid update, got %d : %v", len(errors), errors)
	}
}

// TestServiceCIDRStatusStrategy 验证 /status 允许回写控制器状态，但不允许修改 ServiceCIDR.Spec。
func TestServiceCIDRStatusStrategy(t *testing.T) {
	oldObj := &networking.ServiceCIDR{Spec: networking.ServiceCIDRSpec{}}
	newObj := &networking.ServiceCIDR{
		Spec: networking.ServiceCIDRSpec{
			CIDRs: []string{"10.10.0.0/16"},
		},
	}
	StatusStrategy.PrepareForUpdate(context.TODO(), newObj, oldObj)
	if !reflect.DeepEqual(newObj.Spec, networking.ServiceCIDRSpec{}) {
		t.Errorf("Expected spec field to be preserved from old object during status update")
	}

	newObj = &networking.ServiceCIDR{
		Status: networking.ServiceCIDRStatus{
			Conditions: []metav1.Condition{
				{
					Type:   "bad type",
					Status: "bad status",
				},
			},
		},
	}
	oldObj = &networking.ServiceCIDR{}
	errors := StatusStrategy.ValidateUpdate(context.TODO(), newObj, oldObj)
	if len(errors) != 1 {
		t.Errorf("Expected 1 validation errors for invalid update, got %d", len(errors))
	}
}
