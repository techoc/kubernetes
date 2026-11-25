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

package markcontrolplane

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"

	kubeadmconstants "k8s.io/kubernetes/cmd/kubeadm/app/constants"
)

func TestMarkControlPlane(t *testing.T) {
	// 注意：此测试利用 strategicpatch 对 JSON 的确定性序列化，使 "expectedPatch" 可使用字符串相等比较，
	// 而非 JSON 逻辑相等比较。若 strategicpatch 的行为改变，此处也需相应调整。
	tests := []struct {
		name           string
		existingLabels []string
		existingTaints []v1.Taint
		newTaints      []v1.Taint
		expectedPatch  string
	}{
		{
			name:           "apply default labels",
			existingLabels: []string{""},
			existingTaints: nil,
			newTaints:      nil,
			expectedPatch:  `{"metadata":{"labels":{"node-role.kubernetes.io/control-plane":"","node.kubernetes.io/exclude-from-external-load-balancers":""}}}`,
		},
		{
			name: "control-plane taint missing",
			existingLabels: []string{
				kubeadmconstants.LabelNodeRoleControlPlane,
				kubeadmconstants.LabelExcludeFromExternalLB,
			},
			existingTaints: nil,
			newTaints:      []v1.Taint{kubeadmconstants.ControlPlaneTaint},
			expectedPatch:  `{"spec":{"taints":[{"effect":"NoSchedule","key":"node-role.kubernetes.io/control-plane"}]}}`,
		},
		{
			name: "nothing missing",
			existingLabels: []string{
				kubeadmconstants.LabelNodeRoleControlPlane,
				kubeadmconstants.LabelExcludeFromExternalLB,
			},
			existingTaints: []v1.Taint{kubeadmconstants.ControlPlaneTaint},
			newTaints:      []v1.Taint{kubeadmconstants.ControlPlaneTaint},
			expectedPatch:  `{}`,
		},
		{
			name: "has taint and no new taints wanted",
			existingLabels: []string{
				kubeadmconstants.LabelNodeRoleControlPlane,
				kubeadmconstants.LabelExcludeFromExternalLB,
			},
			existingTaints: []v1.Taint{
				{
					Key:    "node.cloudprovider.kubernetes.io/uninitialized",
					Effect: v1.TaintEffectNoSchedule,
				},
			},
			newTaints:     nil,
			expectedPatch: `{}`,
		},
		{
			name: "has taint and should merge with wanted taint",
			existingLabels: []string{
				kubeadmconstants.LabelNodeRoleControlPlane,
				kubeadmconstants.LabelExcludeFromExternalLB,
			},
			existingTaints: []v1.Taint{
				{
					Key:    "node.cloudprovider.kubernetes.io/uninitialized",
					Effect: v1.TaintEffectNoSchedule,
				},
			},
			newTaints:     []v1.Taint{kubeadmconstants.ControlPlaneTaint},
			expectedPatch: `{"spec":{"taints":[{"effect":"NoSchedule","key":"node-role.kubernetes.io/control-plane"},{"effect":"NoSchedule","key":"node.cloudprovider.kubernetes.io/uninitialized"}]}}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			nodename := "node01"
			controlPlaneNode := &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: nodename,
					Labels: map[string]string{
						v1.LabelHostname: nodename,
					},
				},
			}

			for _, label := range tc.existingLabels {
				controlPlaneNode.ObjectMeta.Labels[label] = ""
			}

			if tc.existingTaints != nil {
				controlPlaneNode.Spec.Taints = tc.existingTaints
			}

			jsonNode, err := json.Marshal(controlPlaneNode)
			if err != nil {
				t.Fatalf("unexpected encoding error: %v", err)
			}

			var patchRequest string
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				w.Header().Set("Content-Type", "application/json")

				if req.URL.Path != "/api/v1/nodes/"+nodename {
					t.Errorf("request for unexpected HTTP resource: %v", req.URL.Path)
					http.Error(w, "", http.StatusNotFound)
					return
				}

				switch req.Method {
				case "GET":
				case "PATCH":
					buf := new(bytes.Buffer)
					buf.ReadFrom(req.Body)
					patchRequest = buf.String()
				default:
					t.Errorf("request for unexpected HTTP verb: %v", req.Method)
					http.Error(w, "", http.StatusNotFound)
					return
				}

				w.WriteHeader(http.StatusOK)
				w.Write(jsonNode)
			}))
			defer s.Close()

			cs, err := clientset.NewForConfig(&restclient.Config{Host: s.URL})
			if err != nil {
				t.Fatalf("unexpected error building clientset: %v", err)
			}

			if err := MarkControlPlane(cs, nodename, tc.newTaints); err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			if tc.expectedPatch != patchRequest {
				t.Errorf("unexpected error: wanted patch %v, got %v", tc.expectedPatch, patchRequest)
			}
		})
	}
}
