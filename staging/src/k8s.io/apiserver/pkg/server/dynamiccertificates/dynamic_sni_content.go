/*
Copyright 2019 The Kubernetes Authors.

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

package dynamiccertificates

// DynamicFileSNIContent provides a SNICertKeyContentProvider that can dynamically react to new file content
// 它把“按文件热加载证书”与“固定 SNI 名称列表”组合在一起：
// 证书可随文件变化重新加载，而 SNI 名称在构造时确定，因为客户端 SNI 的匹配集合必须稳定可预期。
type DynamicFileSNIContent struct {
	*DynamicCertKeyPairContent
	sniNames []string
}

var _ SNICertKeyContentProvider = &DynamicFileSNIContent{}
var _ ControllerRunner = &DynamicFileSNIContent{}

// NewDynamicSNIContentFromFiles returns a dynamic SNICertKeyContentProvider based on a cert and key filename and explicit names
// 构造时立即加载一次证书：启动阶段配置错误应直接失败，而不是等到第一次 TLS 握手。
func NewDynamicSNIContentFromFiles(purpose, certFile, keyFile string, sniNames ...string) (*DynamicFileSNIContent, error) {
	servingContent, err := NewDynamicServingContentFromFiles(purpose, certFile, keyFile)
	if err != nil {
		return nil, err
	}

	ret := &DynamicFileSNIContent{
		DynamicCertKeyPairContent: servingContent,
		sniNames:                  sniNames,
	}
	if err := ret.loadCertKeyPair(); err != nil {
		return nil, err
	}

	return ret, nil
}

// SNINames returns explicitly set SNI names for the certificate. These are not dynamic.
// 返回内部切片而非副本：调用方约定只读，避免每次 TLS 握手分配内存。
func (c *DynamicFileSNIContent) SNINames() []string {
	return c.sniNames
}
