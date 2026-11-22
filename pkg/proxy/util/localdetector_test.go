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

package util

import (
	"reflect"
	"testing"
)

// TestNoOpLocalDetector 验证空实现不生成任何本地流量匹配规则。
func TestNoOpLocalDetector(t *testing.T) {
	localDetector := NewNoOpLocalDetector()
	if localDetector.IsImplemented() {
		t.Error("NoOpLocalDetector returns true for IsImplemented")
	}

	ifLocal := localDetector.IfLocal()
	if len(ifLocal) != 0 {
		t.Errorf("NoOpLocalDetector returns %v for IsLocal (expected nil)", ifLocal)
	}

	ifNotLocal := localDetector.IfNotLocal()
	if len(ifNotLocal) != 0 {
		t.Errorf("NoOpLocalDetector returns %v for IsNotLocal (expected nil)", ifNotLocal)
	}
}

// TestDetectLocalByCIDR 验证按 CIDR 生成的 iptables 匹配参数，覆盖 IPv4 与 IPv6 地址族。
func TestDetectLocalByCIDR(t *testing.T) {
	cases := []struct {
		cidr                     string
		expectedIfLocalOutput    []string
		expectedIfNotLocalOutput []string
	}{
		{
			cidr:                     "10.0.0.0/14",
			expectedIfLocalOutput:    []string{"-s", "10.0.0.0/14"},
			expectedIfNotLocalOutput: []string{"!", "-s", "10.0.0.0/14"},
		},
		{
			// 同时覆盖 IPv6 CIDR：两种地址族在 iptables 中的写法一致。
			cidr:                     "2002:0:0:1234::/64",
			expectedIfLocalOutput:    []string{"-s", "2002:0:0:1234::/64"},
			expectedIfNotLocalOutput: []string{"!", "-s", "2002:0:0:1234::/64"},
		},
	}
	for _, c := range cases {
		localDetector := NewDetectLocalByCIDR(c.cidr)
		if !localDetector.IsImplemented() {
			t.Error("DetectLocalByCIDR returns false for IsImplemented")
		}

		ifLocal := localDetector.IfLocal()
		ifNotLocal := localDetector.IfNotLocal()

		if !reflect.DeepEqual(ifLocal, c.expectedIfLocalOutput) {
			t.Errorf("IfLocal, expected: '%v', but got: '%v'", c.expectedIfLocalOutput, ifLocal)
		}

		if !reflect.DeepEqual(ifNotLocal, c.expectedIfNotLocalOutput) {
			t.Errorf("IfNotLocal, expected: '%v', but got: '%v'", c.expectedIfNotLocalOutput, ifNotLocal)
		}
	}
}

// TestDetectLocalByBridgeInterface 验证按网桥接口名生成的 iptables 匹配参数。
func TestDetectLocalByBridgeInterface(t *testing.T) {
	cases := []struct {
		ifaceName               string
		expectedJumpIfOutput    []string
		expectedJumpIfNotOutput []string
	}{
		{
			ifaceName:               "eth0",
			expectedJumpIfOutput:    []string{"-i", "eth0"},
			expectedJumpIfNotOutput: []string{"!", "-i", "eth0"},
		},
	}
	for _, c := range cases {
		localDetector := NewDetectLocalByBridgeInterface(c.ifaceName)
		if !localDetector.IsImplemented() {
			t.Error("DetectLocalByBridgeInterface returns false for IsImplemented")
		}

		ifLocal := localDetector.IfLocal()
		ifNotLocal := localDetector.IfNotLocal()

		if !reflect.DeepEqual(ifLocal, c.expectedJumpIfOutput) {
			t.Errorf("IfLocal, expected: '%v', but got: '%v'", c.expectedJumpIfOutput, ifLocal)
		}

		if !reflect.DeepEqual(ifNotLocal, c.expectedJumpIfNotOutput) {
			t.Errorf("IfNotLocal, expected: '%v', but got: '%v'", c.expectedJumpIfNotOutput, ifNotLocal)
		}
	}
}

// TestDetectLocalNFTByBridgeInterface 验证按网桥接口名生成的 nftables 匹配参数。
func TestDetectLocalNFTByBridgeInterface(t *testing.T) {
	cases := []struct {
		ifaceName               string
		expectedJumpIfOutput    []string
		expectedJumpIfNotOutput []string
	}{
		{
			ifaceName:               "eth0",
			expectedJumpIfOutput:    []string{"iifname", "eth0"},
			expectedJumpIfNotOutput: []string{"iifname", "!=", "eth0"},
		},
	}
	for _, c := range cases {
		localDetector := NewDetectLocalByBridgeInterface(c.ifaceName)
		if !localDetector.IsImplemented() {
			t.Error("DetectLocalByBridgeInterface returns false for IsImplemented")
		}

		ifLocal := localDetector.IfLocalNFT()
		ifNotLocal := localDetector.IfNotLocalNFT()

		if !reflect.DeepEqual(ifLocal, c.expectedJumpIfOutput) {
			t.Errorf("IfLocalNFT, expected: '%v', but got: '%v'", c.expectedJumpIfOutput, ifLocal)
		}

		if !reflect.DeepEqual(ifNotLocal, c.expectedJumpIfNotOutput) {
			t.Errorf("IfNotLocalNFT, expected: '%v', but got: '%v'", c.expectedJumpIfNotOutput, ifNotLocal)
		}
	}
}

// TestDetectLocalByInterfaceNamePrefix 验证接口名前缀会转换成 iptables 的通配匹配后缀。
func TestDetectLocalByInterfaceNamePrefix(t *testing.T) {
	cases := []struct {
		ifacePrefix             string
		chain                   string
		args                    []string
		expectedJumpIfOutput    []string
		expectedJumpIfNotOutput []string
	}{
		{
			ifacePrefix:             "eth0",
			expectedJumpIfOutput:    []string{"-i", "eth0+"},
			expectedJumpIfNotOutput: []string{"!", "-i", "eth0+"},
		},
	}
	for _, c := range cases {
		localDetector := NewDetectLocalByInterfaceNamePrefix(c.ifacePrefix)
		if !localDetector.IsImplemented() {
			t.Error("DetectLocalByInterfaceNamePrefix returns false for IsImplemented")
		}

		ifLocal := localDetector.IfLocal()
		ifNotLocal := localDetector.IfNotLocal()

		if !reflect.DeepEqual(ifLocal, c.expectedJumpIfOutput) {
			t.Errorf("IfLocal, expected: '%v', but got: '%v'", c.expectedJumpIfOutput, ifLocal)
		}

		if !reflect.DeepEqual(ifNotLocal, c.expectedJumpIfNotOutput) {
			t.Errorf("IfNotLocal, expected: '%v', but got: '%v'", c.expectedJumpIfNotOutput, ifNotLocal)
		}
	}
}

// TestDetectLocalNFTByInterfaceNamePrefix 验证接口名前缀会转换成 nftables 的通配匹配后缀。
func TestDetectLocalNFTByInterfaceNamePrefix(t *testing.T) {
	cases := []struct {
		ifacePrefix             string
		chain                   string
		args                    []string
		expectedJumpIfOutput    []string
		expectedJumpIfNotOutput []string
	}{
		{
			ifacePrefix:             "eth",
			expectedJumpIfOutput:    []string{"iifname", "eth*"},
			expectedJumpIfNotOutput: []string{"iifname", "!=", "eth*"},
		},
	}
	for _, c := range cases {
		localDetector := NewDetectLocalByInterfaceNamePrefix(c.ifacePrefix)
		if !localDetector.IsImplemented() {
			t.Error("DetectLocalByInterfaceNamePrefix returns false for IsImplemented")
		}

		ifLocal := localDetector.IfLocalNFT()
		ifNotLocal := localDetector.IfNotLocalNFT()

		if !reflect.DeepEqual(ifLocal, c.expectedJumpIfOutput) {
			t.Errorf("IfLocalNFT, expected: '%v', but got: '%v'", c.expectedJumpIfOutput, ifLocal)
		}

		if !reflect.DeepEqual(ifNotLocal, c.expectedJumpIfNotOutput) {
			t.Errorf("IfNotLocalNFT, expected: '%v', but got: '%v'", c.expectedJumpIfNotOutput, ifNotLocal)
		}
	}
}
