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

// Package cert 提供证书/私钥文件的读取、写入与解析辅助函数。
package cert

import (
	"crypto/x509"
	"fmt"
	"os"
	"path/filepath"
)

// 若证书与私钥文件都已存在则返回 true，否则返回 false；缺少其中之一会返回错误。
// 证书与私钥必须成对存在，只缺其一会被视为配置错误而非“未配置”。
func CanReadCertAndKey(certPath, keyPath string) (bool, error) {
	certReadable := canReadFile(certPath)
	keyReadable := canReadFile(keyPath)

	if certReadable == false && keyReadable == false {
		return false, nil
	}

	if certReadable == false {
		return false, fmt.Errorf("error reading %s, certificate and key must be supplied as a pair", certPath)
	}

	if keyReadable == false {
		return false, fmt.Errorf("error reading %s, certificate and key must be supplied as a pair", keyPath)
	}

	return true, nil
}

// 若 path 指向的文件存在且可读则返回 true，否则返回 false。
//
// 只打开后立即关闭，不读取内容；调用方需自行处理 TOCTOU 竞态。
func canReadFile(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}

	defer f.Close()

	return true
}

// WriteKey 把 PEM 编码的证书数据写到 certPath，文件权限 0644。
// 证书文件已存在时会被覆盖。
// certPath 的父目录按需以 0755 创建。
func WriteCert(certPath string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(certPath), os.FileMode(0755)); err != nil {
		return err
	}
	return os.WriteFile(certPath, data, os.FileMode(0644))
}

// NewPool 返回包含给定 PEM 文件中证书的 x509.CertPool。
// 文件不可读、证书无法解析或不含任何证书时返回错误。
func NewPool(filename string) (*x509.CertPool, error) {
	pemBlock, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	pool, err := NewPoolFromBytes(pemBlock)
	if err != nil {
		return nil, fmt.Errorf("error creating pool from %s: %s", filename, err)
	}
	return pool, nil
}

// NewPoolFromBytes 返回一个 x509.CertPool，其中包含给定 PEM 编码字节里的证书。
// 若内容无法读取、证书无法解析，或其中不含任何证书则返回错误。
// 会新建一个空池，不会包含系统根证书。
func NewPoolFromBytes(pemBlock []byte) (*x509.CertPool, error) {
	certs, err := ParseCertsPEM(pemBlock)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	for _, cert := range certs {
		pool.AddCert(cert)
	}
	return pool, nil
}

// CertsFromFile 返回给定 PEM 编码文件中包含的 x509.Certificate 列表。
// 若文件无法读取、证书无法解析，或文件中不含任何证书则返回错误。
// 错误信息会带上文件路径，便于定位是哪个证书文件损坏。
func CertsFromFile(file string) ([]*x509.Certificate, error) {
	pemBlock, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	certs, err := ParseCertsPEM(pemBlock)
	if err != nil {
		return nil, fmt.Errorf("error reading %s: %s", file, err)
	}
	return certs, nil
}
