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

package envvars_test

import (
	"reflect"
	"testing"

	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/kubelet/envvars"
)

// TestFromServices 验证由 Service 生成环境变量：命名、协议、多端口、IPv6 与 SCTP 等。
func TestFromServices(t *testing.T) {
	// 构造覆盖单端口、多端口、无名端口、IPv6、SCTP 与无 ClusterIP 的 Service。
	sl := []*v1.Service{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "foo-bar"},
			Spec: v1.ServiceSpec{
				Selector:  map[string]string{"bar": "baz"},
				ClusterIP: "1.2.3.4",
				Ports: []v1.ServicePort{
					{Port: 8080, Protocol: "TCP"},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "abc-123"},
			Spec: v1.ServiceSpec{
				Selector:  map[string]string{"bar": "baz"},
				ClusterIP: "5.6.7.8",
				Ports: []v1.ServicePort{
					{Name: "u-d-p", Port: 8081, Protocol: "UDP"},
					{Name: "t-c-p", Port: 8081, Protocol: "TCP"},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "q-u-u-x"},
			Spec: v1.ServiceSpec{
				Selector:  map[string]string{"bar": "baz"},
				ClusterIP: "9.8.7.6",
				Ports: []v1.ServicePort{
					{Port: 8082, Protocol: "TCP"},
					{Name: "8083", Port: 8083, Protocol: "TCP"},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "svrc-clusterip-none"},
			Spec: v1.ServiceSpec{
				Selector:  map[string]string{"bar": "baz"},
				ClusterIP: "None",
				Ports: []v1.ServicePort{
					{Port: 8082, Protocol: "TCP"},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "svrc-clusterip-empty"},
			Spec: v1.ServiceSpec{
				Selector:  map[string]string{"bar": "baz"},
				ClusterIP: "",
				Ports: []v1.ServicePort{
					{Port: 8082, Protocol: "TCP"},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "super-ipv6"},
			Spec: v1.ServiceSpec{
				Selector:  map[string]string{"bar": "baz"},
				ClusterIP: "2001:DB8::",
				Ports: []v1.ServicePort{
					{Name: "u-d-p", Port: 8084, Protocol: "UDP"},
					{Name: "t-c-p", Port: 8084, Protocol: "TCP"},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "sctp-1"},
			Spec: v1.ServiceSpec{
				Selector:  map[string]string{"bar": "sctp-sel"},
				ClusterIP: "1.2.3.4",
				Ports: []v1.ServicePort{
					{Port: 777, Protocol: "SCTP"},
				},
			},
		},
	}
	// 调用被测函数生成环境变量。
	vars := envvars.FromServices(sl)
	// 期望值的顺序与生成顺序一致，便于逐项比较。
	expected := []v1.EnvVar{
		{Name: "FOO_BAR_SERVICE_HOST", Value: "1.2.3.4"},
		{Name: "FOO_BAR_SERVICE_PORT", Value: "8080"},
		{Name: "FOO_BAR_PORT", Value: "tcp://1.2.3.4:8080"},
		{Name: "FOO_BAR_PORT_8080_TCP", Value: "tcp://1.2.3.4:8080"},
		{Name: "FOO_BAR_PORT_8080_TCP_PROTO", Value: "tcp"},
		{Name: "FOO_BAR_PORT_8080_TCP_PORT", Value: "8080"},
		{Name: "FOO_BAR_PORT_8080_TCP_ADDR", Value: "1.2.3.4"},
		{Name: "ABC_123_SERVICE_HOST", Value: "5.6.7.8"},
		{Name: "ABC_123_SERVICE_PORT", Value: "8081"},
		{Name: "ABC_123_SERVICE_PORT_U_D_P", Value: "8081"},
		{Name: "ABC_123_SERVICE_PORT_T_C_P", Value: "8081"},
		{Name: "ABC_123_PORT", Value: "udp://5.6.7.8:8081"},
		{Name: "ABC_123_PORT_8081_UDP", Value: "udp://5.6.7.8:8081"},
		{Name: "ABC_123_PORT_8081_UDP_PROTO", Value: "udp"},
		{Name: "ABC_123_PORT_8081_UDP_PORT", Value: "8081"},
		{Name: "ABC_123_PORT_8081_UDP_ADDR", Value: "5.6.7.8"},
		{Name: "ABC_123_PORT_8081_TCP", Value: "tcp://5.6.7.8:8081"},
		{Name: "ABC_123_PORT_8081_TCP_PROTO", Value: "tcp"},
		{Name: "ABC_123_PORT_8081_TCP_PORT", Value: "8081"},
		{Name: "ABC_123_PORT_8081_TCP_ADDR", Value: "5.6.7.8"},
		{Name: "Q_U_U_X_SERVICE_HOST", Value: "9.8.7.6"},
		{Name: "Q_U_U_X_SERVICE_PORT", Value: "8082"},
		{Name: "Q_U_U_X_SERVICE_PORT_8083", Value: "8083"},
		{Name: "Q_U_U_X_PORT", Value: "tcp://9.8.7.6:8082"},
		{Name: "Q_U_U_X_PORT_8082_TCP", Value: "tcp://9.8.7.6:8082"},
		{Name: "Q_U_U_X_PORT_8082_TCP_PROTO", Value: "tcp"},
		{Name: "Q_U_U_X_PORT_8082_TCP_PORT", Value: "8082"},
		{Name: "Q_U_U_X_PORT_8082_TCP_ADDR", Value: "9.8.7.6"},
		{Name: "Q_U_U_X_PORT_8083_TCP", Value: "tcp://9.8.7.6:8083"},
		{Name: "Q_U_U_X_PORT_8083_TCP_PROTO", Value: "tcp"},
		{Name: "Q_U_U_X_PORT_8083_TCP_PORT", Value: "8083"},
		{Name: "Q_U_U_X_PORT_8083_TCP_ADDR", Value: "9.8.7.6"},
		{Name: "SUPER_IPV6_SERVICE_HOST", Value: "2001:DB8::"},
		{Name: "SUPER_IPV6_SERVICE_PORT", Value: "8084"},
		{Name: "SUPER_IPV6_SERVICE_PORT_U_D_P", Value: "8084"},
		{Name: "SUPER_IPV6_SERVICE_PORT_T_C_P", Value: "8084"},
		{Name: "SUPER_IPV6_PORT", Value: "udp://[2001:DB8::]:8084"},
		{Name: "SUPER_IPV6_PORT_8084_UDP", Value: "udp://[2001:DB8::]:8084"},
		{Name: "SUPER_IPV6_PORT_8084_UDP_PROTO", Value: "udp"},
		{Name: "SUPER_IPV6_PORT_8084_UDP_PORT", Value: "8084"},
		{Name: "SUPER_IPV6_PORT_8084_UDP_ADDR", Value: "2001:DB8::"},
		{Name: "SUPER_IPV6_PORT_8084_TCP", Value: "tcp://[2001:DB8::]:8084"},
		{Name: "SUPER_IPV6_PORT_8084_TCP_PROTO", Value: "tcp"},
		{Name: "SUPER_IPV6_PORT_8084_TCP_PORT", Value: "8084"},
		{Name: "SUPER_IPV6_PORT_8084_TCP_ADDR", Value: "2001:DB8::"},
		{Name: "SCTP_1_SERVICE_HOST", Value: "1.2.3.4"},
		{Name: "SCTP_1_SERVICE_PORT", Value: "777"},
		{Name: "SCTP_1_PORT", Value: "sctp://1.2.3.4:777"},
		{Name: "SCTP_1_PORT_777_SCTP", Value: "sctp://1.2.3.4:777"},
		{Name: "SCTP_1_PORT_777_SCTP_PROTO", Value: "sctp"},
		{Name: "SCTP_1_PORT_777_SCTP_PORT", Value: "777"},
		{Name: "SCTP_1_PORT_777_SCTP_ADDR", Value: "1.2.3.4"},
	}
	// 先比较数量，避免逐项比较时越界。
	if len(vars) != len(expected) {
		t.Errorf("Expected %d env vars, got: %+v", len(expected), vars)
		return
	}
	// 逐项深比较名称与值。
	for i := range expected {
		if !reflect.DeepEqual(vars[i], expected[i]) {
			t.Errorf("expected %#v, got %#v", vars[i], expected[i])
		}
	}
}
