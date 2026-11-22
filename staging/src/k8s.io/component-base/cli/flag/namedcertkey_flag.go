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

package flag

import (
	"errors"
	"flag"
	"strings"
)

// NamedCertKey 是一个 flag 值，解析 "certfile,keyfile" 或
// "certfile,keyfile:name,name,name" 这两种形式。
// Names 对应 TLS SNI：服务端据此为不同 SNI 名字选择不同证书。
type NamedCertKey struct {
	Names             []string
	CertFile, KeyFile string
}

var _ flag.Value = &NamedCertKey{}

// String 输出 flag 的规范形式；仅当存在 SNI 名字时才追加 ":" 与逗号分隔的名字。
func (nkc *NamedCertKey) String() string {
	s := nkc.CertFile + "," + nkc.KeyFile
	if len(nkc.Names) > 0 {
		s = s + ":" + strings.Join(nkc.Names, ",")
	}
	return s
}

// Set 解析单个证书/私钥组合。冒号后是 SNI 名字列表，逗号前必须是恰好两个
// 路径（证书、私钥），多余或缺失都视为参数错误。
func (nkc *NamedCertKey) Set(value string) error {
	// 只按第一个冒号切分，避免名字部分未来含冒号时被误拆。
	cs := strings.SplitN(value, ":", 2)
	var keycert string
	if len(cs) == 2 {
		var names string
		keycert, names = strings.TrimSpace(cs[0]), strings.TrimSpace(cs[1])
		// 显式写了冒号却没有名字几乎必然是笔误，直接拒绝。
		if names == "" {
			return errors.New("empty names list is not allowed")
		}
		nkc.Names = nil
		for _, name := range strings.Split(names, ",") {
			nkc.Names = append(nkc.Names, strings.TrimSpace(name))
		}
	} else {
		nkc.Names = nil
		keycert = strings.TrimSpace(cs[0])
	}
	cs = strings.Split(keycert, ",")
	if len(cs) != 2 {
		return errors.New("expected comma separated certificate and key file paths")
	}
	nkc.CertFile = strings.TrimSpace(cs[0])
	nkc.KeyFile = strings.TrimSpace(cs[1])
	return nil
}

// Type 返回该 flag 在帮助信息中展示的类型名。
func (*NamedCertKey) Type() string {
	return "namedCertKey"
}

// NamedCertKeyArray 是一个 flag 值，每个 NamedCertKey 通过独立的一次
// flag 出现来传入（与逗号分隔的 slice 不同），因此可以重复指定该 flag。
type NamedCertKeyArray struct {
	value   *[]NamedCertKey
	changed bool
}

var _ flag.Value = &NamedCertKeyArray{}

// NewNamedCertKeyArray 创建 NamedCertKeyArray，内部值指向 p。
// 注意函数名沿用历史上的 NewNamedKeyCertArray，实际签名一致。
func NewNamedCertKeyArray(p *[]NamedCertKey) *NamedCertKeyArray {
	return &NamedCertKeyArray{
		value: p,
	}
}

// Set 解析一个证书/私钥对并追加到目标 slice。
// 首次调用会覆盖调用方预置的默认值，这是重复 flag 的语义：
// 命令行出现即表示完全替换默认配置。
func (a *NamedCertKeyArray) Set(val string) error {
	nkc := NamedCertKey{}
	err := nkc.Set(val)
	if err != nil {
		return err
	}
	if !a.changed {
		*a.value = []NamedCertKey{nkc}
		a.changed = true
	} else {
		*a.value = append(*a.value, nkc)
	}
	return nil
}

// Type 返回该 flag 在帮助信息中展示的类型名。
func (a *NamedCertKeyArray) Type() string {
	return "namedCertKey"
}

// String 用 ";" 连接每个证书项并加方括号，作为整体展示形式。
func (a *NamedCertKeyArray) String() string {
	nkcs := make([]string, 0, len(*a.value))
	for i := range *a.value {
		nkcs = append(nkcs, (*a.value)[i].String())
	}
	return "[" + strings.Join(nkcs, ";") + "]"
}
