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

package openshift

import (
	"testing"

	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/kubernetes/pkg/controlplane"
	controlplaneapiserver "k8s.io/kubernetes/pkg/controlplane/apiserver"
)

// 这个测试引用了 OpenShift 在启动时用于定制 apiserver 的方法，
// 这些方法不会被实例直接引用。
func TestApiserverExportsSymbols(t *testing.T) {
	_ = &controlplane.Config{
		ControlPlane: controlplaneapiserver.Config{
			Generic: &genericapiserver.Config{
				EnableMetrics: true,
			},
			Extra: controlplaneapiserver.Extra{
				EnableLogsSupport: false,
			},
		},
	}
	_ = &controlplane.Instance{
		ControlPlane: &controlplaneapiserver.Server{
			GenericAPIServer: &genericapiserver.GenericAPIServer{},
		},
	}
}
