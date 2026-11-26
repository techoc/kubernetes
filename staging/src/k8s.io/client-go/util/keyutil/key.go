/*
Copyright 2018 The Kubernetes Authors.

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

// Package keyutil 提供管理公私钥对的工具。
package keyutil

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	cryptorand "crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
)

const (
	// ECPrivateKeyBlockType 是 pem.Block.Type 的可能取值。
	ECPrivateKeyBlockType = "EC PRIVATE KEY"
	// RSAPrivateKeyBlockType 是 pem.Block.Type 的可能取值。
	RSAPrivateKeyBlockType = "RSA PRIVATE KEY"
	// PrivateKeyBlockType 是 pem.Block.Type 的可能取值。
	PrivateKeyBlockType = "PRIVATE KEY"
	// PublicKeyBlockType 是 pem.Block.Type 的可能取值。
	PublicKeyBlockType = "PUBLIC KEY"
)

// MakeEllipticPrivateKeyPEM 创建 ECDSA 私钥。
//
// 固定使用 P-256：兼容性最好，且比 RSA 生成更快、密钥更短。
func MakeEllipticPrivateKeyPEM() ([]byte, error) {
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), cryptorand.Reader)
	if err != nil {
		return nil, err
	}

	derBytes, err := x509.MarshalECPrivateKey(privateKey)
	if err != nil {
		return nil, err
	}

	privateKeyPemBlock := &pem.Block{
		Type:  ECPrivateKeyBlockType,
		Bytes: derBytes,
	}
	return pem.EncodeToMemory(privateKeyPemBlock), nil
}

// WriteKey 把 PEM 编码的密钥写到 keyPath，文件权限 0600，已存在则覆盖；父目录按需
// 以 0755 创建。
//
// 私钥文件权限必须是 0600，避免同机其他用户读取；已存在的文件会被原地
// 覆盖而不是创建新 inode，权限保持不变。
func WriteKey(keyPath string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(keyPath), os.FileMode(0755)); err != nil {
		return err
	}
	return os.WriteFile(keyPath, data, os.FileMode(0600))
}

// LoadOrGenerateKeyFile 从指定路径读取密钥；找不到时生成新密钥并写入该路径。
//
// 文件存在但内容损坏时不会覆盖，而是报错：静默替换会破坏依赖旧公钥的
// 系统，应让运维显式决定如何处理。
func LoadOrGenerateKeyFile(keyPath string) (data []byte, wasGenerated bool, err error) {
	loadedData, err := os.ReadFile(keyPath)
	// 调用 verifyKeyData 确认文件非空且未损坏。
	if err == nil && verifyKeyData(loadedData) {
		return loadedData, false, err
	}
	if !os.IsNotExist(err) {
		return nil, false, fmt.Errorf("error loading key from %s: %v", keyPath, err)
	}

	generatedData, err := MakeEllipticPrivateKeyPEM()
	if err != nil {
		return nil, false, fmt.Errorf("error generating key: %v", err)
	}
	if err := WriteKey(keyPath, generatedData); err != nil {
		return nil, false, fmt.Errorf("error writing key to %s: %v", keyPath, err)
	}
	return generatedData, true, nil
}

// MarshalPrivateKeyToPEM 把已知的 RSA 或 ECDSA 私钥转换为 PEM 编码块，失败返回错误。
func MarshalPrivateKeyToPEM(privateKey crypto.PrivateKey) ([]byte, error) {
	// RSA 使用 PKCS#1 块类型，ECDSA 使用 SEC1；这是历史兼容格式，
	// 下游工具普遍能识别。
	switch t := privateKey.(type) {
	case *ecdsa.PrivateKey:
		derBytes, err := x509.MarshalECPrivateKey(t)
		if err != nil {
			return nil, err
		}
		block := &pem.Block{
			Type:  ECPrivateKeyBlockType,
			Bytes: derBytes,
		}
		return pem.EncodeToMemory(block), nil
	case *rsa.PrivateKey:
		block := &pem.Block{
			Type:  RSAPrivateKeyBlockType,
			Bytes: x509.MarshalPKCS1PrivateKey(t),
		}
		return pem.EncodeToMemory(block), nil
	default:
		return nil, fmt.Errorf("private key is not a recognized type: %T", privateKey)
	}
}

// PrivateKeyFromFile 从 PEM 文件读取 RSA 或 ECDSA 私钥；文件不可读或密钥无法解析时
// 返回错误。
func PrivateKeyFromFile(file string) (interface{}, error) {
	data, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	key, err := ParsePrivateKeyPEM(data)
	if err != nil {
		return nil, fmt.Errorf("error reading private key file %s: %v", file, err)
	}
	return key, nil
}

// PublicKeysFromFile 从 PEM 文件读取 RSA 或 ECDSA 公钥；公钥文件与私钥文件都会读取。
func PublicKeysFromFile(file string) ([]interface{}, error) {
	data, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	keys, err := ParsePublicKeysPEM(data)
	if err != nil {
		return nil, fmt.Errorf("error reading public key file %s: %v", file, err)
	}
	return keys, nil
}

// verifyKeyData 判断给定数据看起来是否是有效私钥。
func verifyKeyData(data []byte) bool {
	if len(data) == 0 {
		return false
	}
	_, err := ParsePrivateKeyPEM(data)
	return err == nil
}

// ParsePrivateKeyPEM 从给定数据的 PEM 块中解析私钥，识别 "EC PRIVATE KEY"、
// "RSA PRIVATE KEY" 与 "PRIVATE KEY"。
//
// 会跳过无法解析的块继续查找，因此文件里混有证书、参数块也能正常工作。
func ParsePrivateKeyPEM(keyData []byte) (interface{}, error) {
	var privateKeyPemBlock *pem.Block
	for {
		privateKeyPemBlock, keyData = pem.Decode(keyData)
		if privateKeyPemBlock == nil {
			break
		}

		switch privateKeyPemBlock.Type {
		case ECPrivateKeyBlockType:
			// ASN.1 格式的 ECDSA 私钥。
			if key, err := x509.ParseECPrivateKey(privateKeyPemBlock.Bytes); err == nil {
				return key, nil
			}
		case RSAPrivateKeyBlockType:
			// PKCS#1 格式的 RSA 私钥。
			if key, err := x509.ParsePKCS1PrivateKey(privateKeyPemBlock.Bytes); err == nil {
				return key, nil
			}
		case PrivateKeyBlockType:
			// 未加密 PKCS#8 格式的 RSA 或 ECDSA 私钥。
			if key, err := x509.ParsePKCS8PrivateKey(privateKeyPemBlock.Bytes); err == nil {
				return key, nil
			}
		}

		// 容忍非密钥 PEM 块（如 "EC PARAMETERS"）以兼容历史数据；最初只解析第一个块并
		// 期望它是密钥块。
	}

	// 读完所有 PEM 块仍未识别到密钥。
	return nil, fmt.Errorf("data does not contain a valid RSA or ECDSA private key")
}

// ParsePublicKeysPEM 从 PEM 编码字节中读取 RSA 或 ECDSA 公钥数组；公钥文件与私钥
// 文件都会读取。
//
// 私钥文件也可以提取公钥，方便调用方只持有一个密钥文件；无法识别的块被
// 忽略以兼容附带注释或其他 PEM 内容的文件。
func ParsePublicKeysPEM(keyData []byte) ([]interface{}, error) {
	var block *pem.Block
	keys := []interface{}{}
	for {
		// 读取下一个块。
		block, keyData = pem.Decode(keyData)
		if block == nil {
			break
		}

		// 用各解析函数尝试解析该块。
		if privateKey, err := parseRSAPrivateKey(block.Bytes); err == nil {
			keys = append(keys, &privateKey.PublicKey)
			continue
		}
		if publicKey, err := parseRSAPublicKey(block.Bytes); err == nil {
			keys = append(keys, publicKey)
			continue
		}
		if privateKey, err := parseECPrivateKey(block.Bytes); err == nil {
			keys = append(keys, &privateKey.PublicKey)
			continue
		}
		if publicKey, err := parseECPublicKey(block.Bytes); err == nil {
			keys = append(keys, publicKey)
			continue
		}

		// 为向后兼容容忍非密钥 PEM 块；最初只解析第一个块并期望它是密钥块。
	}

	if len(keys) == 0 {
		return nil, fmt.Errorf("data does not contain any valid RSA or ECDSA public keys")
	}
	return keys, nil
}

// parseRSAPublicKey 从给定数据解析单个 RSA 公钥。
//
// 先按 PKIX 解析，失败再尝试把数据当作证书取其中公钥；这是为了兼容
// 直接粘贴 PEM 证书的旧用法。
func parseRSAPublicKey(data []byte) (*rsa.PublicKey, error) {
	var err error

	// 解析密钥。
	var parsedKey interface{}
	if parsedKey, err = x509.ParsePKIXPublicKey(data); err != nil {
		if cert, err := x509.ParseCertificate(data); err == nil {
			parsedKey = cert.PublicKey
		} else {
			return nil, err
		}
	}

	// 检查解析结果是否为 RSA 公钥。
	var pubKey *rsa.PublicKey
	var ok bool
	if pubKey, ok = parsedKey.(*rsa.PublicKey); !ok {
		return nil, fmt.Errorf("data doesn't contain valid RSA Public Key")
	}

	return pubKey, nil
}

// parseRSAPrivateKey 从给定数据解析单个 RSA 私钥。
//
// 兼容 PKCS#1 与传统 PKCS#8 两种编码。
func parseRSAPrivateKey(data []byte) (*rsa.PrivateKey, error) {
	var err error

	// 解析密钥。
	var parsedKey interface{}
	if parsedKey, err = x509.ParsePKCS1PrivateKey(data); err != nil {
		if parsedKey, err = x509.ParsePKCS8PrivateKey(data); err != nil {
			return nil, err
		}
	}

	// 检查解析结果是否为 RSA 私钥。
	var privKey *rsa.PrivateKey
	var ok bool
	if privKey, ok = parsedKey.(*rsa.PrivateKey); !ok {
		return nil, fmt.Errorf("data doesn't contain valid RSA Private Key")
	}

	return privKey, nil
}

// parseECPublicKey 从给定数据解析单个 ECDSA 公钥。
func parseECPublicKey(data []byte) (*ecdsa.PublicKey, error) {
	var err error

	// 解析密钥。
	var parsedKey interface{}
	if parsedKey, err = x509.ParsePKIXPublicKey(data); err != nil {
		if cert, err := x509.ParseCertificate(data); err == nil {
			parsedKey = cert.PublicKey
		} else {
			return nil, err
		}
	}

	// 检查解析结果是否为 ECDSA 公钥。
	var pubKey *ecdsa.PublicKey
	var ok bool
	if pubKey, ok = parsedKey.(*ecdsa.PublicKey); !ok {
		return nil, fmt.Errorf("data doesn't contain valid ECDSA Public Key")
	}

	return pubKey, nil
}

// parseECPrivateKey 从给定数据解析单个 ECDSA 私钥。
func parseECPrivateKey(data []byte) (*ecdsa.PrivateKey, error) {
	var err error

	// 解析密钥。
	var parsedKey interface{}
	if parsedKey, err = x509.ParseECPrivateKey(data); err != nil {
		return nil, err
	}

	// 检查解析结果是否为 ECDSA 私钥。
	var privKey *ecdsa.PrivateKey
	var ok bool
	if privKey, ok = parsedKey.(*ecdsa.PrivateKey); !ok {
		return nil, fmt.Errorf("data doesn't contain valid ECDSA Private Key")
	}

	return privKey, nil
}
