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

// Package keyutil contains utilities for managing public/private key pairs.
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
	// ECPrivateKeyBlockType is a possible value for pem.Block.Type.
	ECPrivateKeyBlockType = "EC PRIVATE KEY"
	// RSAPrivateKeyBlockType is a possible value for pem.Block.Type.
	RSAPrivateKeyBlockType = "RSA PRIVATE KEY"
	// PrivateKeyBlockType is a possible value for pem.Block.Type.
	PrivateKeyBlockType = "PRIVATE KEY"
	// PublicKeyBlockType is a possible value for pem.Block.Type.
	PublicKeyBlockType = "PUBLIC KEY"
)

// MakeEllipticPrivateKeyPEM creates an ECDSA private key
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

// WriteKey writes the pem-encoded key data to keyPath.
// The key file will be created with file mode 0600.
// If the key file already exists, it will be overwritten.
// The parent directory of the keyPath will be created as needed with file mode 0755.
//
// 私钥文件权限必须是 0600，避免同机其他用户读取；已存在的文件会被原地
// 覆盖而不是创建新 inode，权限保持不变。
func WriteKey(keyPath string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(keyPath), os.FileMode(0755)); err != nil {
		return err
	}
	return os.WriteFile(keyPath, data, os.FileMode(0600))
}

// LoadOrGenerateKeyFile looks for a key in the file at the given path. If it
// can't find one, it will generate a new key and store it there.
//
// 文件存在但内容损坏时不会覆盖，而是报错：静默替换会破坏依赖旧公钥的
// 系统，应让运维显式决定如何处理。
func LoadOrGenerateKeyFile(keyPath string) (data []byte, wasGenerated bool, err error) {
	loadedData, err := os.ReadFile(keyPath)
	// Call verifyKeyData to ensure the file wasn't empty/corrupt.
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

// MarshalPrivateKeyToPEM converts a known private key type of RSA or ECDSA to
// a PEM encoded block or returns an error.
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

// PrivateKeyFromFile returns the private key in rsa.PrivateKey or ecdsa.PrivateKey format from a given PEM-encoded file.
// Returns an error if the file could not be read or if the private key could not be parsed.
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

// PublicKeysFromFile returns the public keys in rsa.PublicKey or ecdsa.PublicKey format from a given PEM-encoded file.
// Reads public keys from both public and private key files.
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

// verifyKeyData returns true if the provided data appears to be a valid private key.
func verifyKeyData(data []byte) bool {
	if len(data) == 0 {
		return false
	}
	_, err := ParsePrivateKeyPEM(data)
	return err == nil
}

// ParsePrivateKeyPEM returns a private key parsed from a PEM block in the supplied data.
// Recognizes PEM blocks for "EC PRIVATE KEY", "RSA PRIVATE KEY", or "PRIVATE KEY"
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
			// ECDSA Private Key in ASN.1 format
			if key, err := x509.ParseECPrivateKey(privateKeyPemBlock.Bytes); err == nil {
				return key, nil
			}
		case RSAPrivateKeyBlockType:
			// RSA Private Key in PKCS#1 format
			if key, err := x509.ParsePKCS1PrivateKey(privateKeyPemBlock.Bytes); err == nil {
				return key, nil
			}
		case PrivateKeyBlockType:
			// RSA or ECDSA Private Key in unencrypted PKCS#8 format
			if key, err := x509.ParsePKCS8PrivateKey(privateKeyPemBlock.Bytes); err == nil {
				return key, nil
			}
		}

		// tolerate non-key PEM blocks for compatibility with things like "EC PARAMETERS" blocks
		// originally, only the first PEM block was parsed and expected to be a key block
	}

	// we read all the PEM blocks and didn't recognize one
	return nil, fmt.Errorf("data does not contain a valid RSA or ECDSA private key")
}

// ParsePublicKeysPEM is a helper function for reading an array of rsa.PublicKey or ecdsa.PublicKey from a PEM-encoded byte array.
// Reads public keys from both public and private key files.
//
// 私钥文件也可以提取公钥，方便调用方只持有一个密钥文件；无法识别的块被
// 忽略以兼容附带注释或其他 PEM 内容的文件。
func ParsePublicKeysPEM(keyData []byte) ([]interface{}, error) {
	var block *pem.Block
	keys := []interface{}{}
	for {
		// read the next block
		block, keyData = pem.Decode(keyData)
		if block == nil {
			break
		}

		// test block against parsing functions
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

		// tolerate non-key PEM blocks for backwards compatibility
		// originally, only the first PEM block was parsed and expected to be a key block
	}

	if len(keys) == 0 {
		return nil, fmt.Errorf("data does not contain any valid RSA or ECDSA public keys")
	}
	return keys, nil
}

// parseRSAPublicKey parses a single RSA public key from the provided data
//
// 先按 PKIX 解析，失败再尝试把数据当作证书取其中公钥；这是为了兼容
// 直接粘贴 PEM 证书的旧用法。
func parseRSAPublicKey(data []byte) (*rsa.PublicKey, error) {
	var err error

	// Parse the key
	var parsedKey interface{}
	if parsedKey, err = x509.ParsePKIXPublicKey(data); err != nil {
		if cert, err := x509.ParseCertificate(data); err == nil {
			parsedKey = cert.PublicKey
		} else {
			return nil, err
		}
	}

	// Test if parsed key is an RSA Public Key
	var pubKey *rsa.PublicKey
	var ok bool
	if pubKey, ok = parsedKey.(*rsa.PublicKey); !ok {
		return nil, fmt.Errorf("data doesn't contain valid RSA Public Key")
	}

	return pubKey, nil
}

// parseRSAPrivateKey parses a single RSA private key from the provided data
//
// 兼容 PKCS#1 与传统 PKCS#8 两种编码。
func parseRSAPrivateKey(data []byte) (*rsa.PrivateKey, error) {
	var err error

	// Parse the key
	var parsedKey interface{}
	if parsedKey, err = x509.ParsePKCS1PrivateKey(data); err != nil {
		if parsedKey, err = x509.ParsePKCS8PrivateKey(data); err != nil {
			return nil, err
		}
	}

	// Test if parsed key is an RSA Private Key
	var privKey *rsa.PrivateKey
	var ok bool
	if privKey, ok = parsedKey.(*rsa.PrivateKey); !ok {
		return nil, fmt.Errorf("data doesn't contain valid RSA Private Key")
	}

	return privKey, nil
}

// parseECPublicKey parses a single ECDSA public key from the provided data
func parseECPublicKey(data []byte) (*ecdsa.PublicKey, error) {
	var err error

	// Parse the key
	var parsedKey interface{}
	if parsedKey, err = x509.ParsePKIXPublicKey(data); err != nil {
		if cert, err := x509.ParseCertificate(data); err == nil {
			parsedKey = cert.PublicKey
		} else {
			return nil, err
		}
	}

	// Test if parsed key is an ECDSA Public Key
	var pubKey *ecdsa.PublicKey
	var ok bool
	if pubKey, ok = parsedKey.(*ecdsa.PublicKey); !ok {
		return nil, fmt.Errorf("data doesn't contain valid ECDSA Public Key")
	}

	return pubKey, nil
}

// parseECPrivateKey parses a single ECDSA private key from the provided data
func parseECPrivateKey(data []byte) (*ecdsa.PrivateKey, error) {
	var err error

	// Parse the key
	var parsedKey interface{}
	if parsedKey, err = x509.ParseECPrivateKey(data); err != nil {
		return nil, err
	}

	// Test if parsed key is an ECDSA Private Key
	var privKey *ecdsa.PrivateKey
	var ok bool
	if privKey, ok = parsedKey.(*ecdsa.PrivateKey); !ok {
		return nil, fmt.Errorf("data doesn't contain valid ECDSA Private Key")
	}

	return privKey, nil
}
