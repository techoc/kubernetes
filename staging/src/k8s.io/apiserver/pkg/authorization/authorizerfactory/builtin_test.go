/*
Copyright 2016 The Kubernetes Authors.

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

package authorizerfactory

import (
	"context"
	"testing"

	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
)

// TestNewAlwaysAllowAuthorizer 验证 AlwaysAllow 授权器无条件放行。
func TestNewAlwaysAllowAuthorizer(t *testing.T) {
	aaa := NewAlwaysAllowAuthorizer()
	if decision, _, _ := aaa.Authorize(context.Background(), nil); decision != authorizer.DecisionAllow {
		t.Errorf("AlwaysAllowAuthorizer.Authorize did not authorize successfully.")
	}
}

// TestNewAlwaysDenyAuthorizer 验证 AlwaysDeny 授权器不会返回 Allow。
func TestNewAlwaysDenyAuthorizer(t *testing.T) {
	ada := NewAlwaysDenyAuthorizer()
	if decision, _, _ := ada.Authorize(context.Background(), nil); decision == authorizer.DecisionAllow {
		t.Errorf("AlwaysDenyAuthorizer.Authorize returned nil instead of error.")
	}
}

// TestPrivilegedGroupAuthorizer 验证只有属于特权组的用户被放行。
// 重复传入同一组名用于确认实现不会因重复项产生异常。
func TestPrivilegedGroupAuthorizer(t *testing.T) {
	auth := NewPrivilegedGroups("allow-01", "allow-01")

	yes := authorizer.AttributesRecord{User: &user.DefaultInfo{Groups: []string{"no", "allow-01"}}}
	no := authorizer.AttributesRecord{User: &user.DefaultInfo{Groups: []string{"no", "deny-01"}}}

	if authorized, _, _ := auth.Authorize(context.Background(), yes); authorized != authorizer.DecisionAllow {
		t.Errorf("failed")
	}
	if authorized, _, _ := auth.Authorize(context.Background(), no); authorized == authorizer.DecisionAllow {
		t.Errorf("failed")
	}
}
