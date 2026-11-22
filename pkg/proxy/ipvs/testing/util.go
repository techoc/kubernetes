//go:build linux

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

package testing

import (
	utilipset "k8s.io/kubernetes/pkg/proxy/ipvs/ipset"
)

// ExpectedVirtualServer 描述测试中期望的 ipvs 规则，包含虚拟服务器（VS）及其真实服务器（RS）。
// VSNum 是期望的 ipvs 虚拟服务器数量，IP、Port、Protocol 描述该虚拟服务器的信息；
// RS 是该虚拟服务器对应的真实服务器列表。
type ExpectedVirtualServer struct {
	VSNum    int
	IP       string
	Port     uint16
	Protocol string
	RS       []ExpectedRealServer
}

// ExpectedRealServer 描述一条期望的 ipvs 真实服务器（RS）记录。
type ExpectedRealServer struct {
	IP   string
	Port uint16
}

// ExpectedIptablesChain 以链名为键，保存该链上期望出现的跳转规则列表。
type ExpectedIptablesChain map[string][]ExpectedIptablesRule

// ExpectedIptablesRule 描述一条期望的 iptables 规则，包括源地址、跳转目标链及要匹配的 ipset 名称。
type ExpectedIptablesRule struct {
	SourceAddress string
	JumpChain     string
	MatchSet      string
}

// ExpectedIPSet 以集合名为键，保存该集合中期望存在的条目列表。
type ExpectedIPSet map[string][]*utilipset.Entry
