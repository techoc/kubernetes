/*
Copyright 2024 The Kubernetes Authors.

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

package v1

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "k8s.io/externaljwt/apis/v1"
	"k8s.io/klog/v2"
)

// MockSigner 是在 Unix Socket 上提供 ExternalJWTSigner 服务的测试替身。
// 通过公开字段可控制签名配置和故障注入；NewMockSigner 会注册清理函数。
type MockSigner struct {
	socketPath string
	server     *grpc.Server
	listener   net.Listener

	SigningKey                *rsa.PrivateKey
	SigningKeyID              string
	SigningAlg                string
	TokenType                 string
	MaxTokenExpirationSeconds int64

	supportedKeys        map[string]KeyT
	supportedKeysLock    sync.RWMutex
	supportedKeysFetched *sync.Cond

	FetchError    error
	MetadataError error
	errorLock     sync.RWMutex

	v1.UnsafeExternalJWTSignerServer
}

// KeyT 描述 MockSigner 返回的一个公钥及其 OIDC 发现可见性。
type KeyT struct {
	Key                      []byte
	ExcludeFromOidcDiscovery bool
}

// NewMockSigner 启动并返回在指定 Unix Socket 上提供服务的 MockSigner。
// 初始化密钥后等待 gRPC 服务就绪，并通过 testing.T 注册资源清理。
func NewMockSigner(t *testing.T, socketPath string) *MockSigner {
	server := grpc.NewServer()

	m := &MockSigner{
		socketPath:                socketPath,
		server:                    server,
		MaxTokenExpirationSeconds: 10 * 60, // 10m
	}
	m.supportedKeysFetched = sync.NewCond(&m.supportedKeysLock)

	if err := m.Reset(); err != nil {
		t.Fatalf("failed to load keys for mock signer: %v", err)
	}

	v1.RegisterExternalJWTSignerServer(server, m)
	if err := m.start(t); err != nil {
		t.Fatalf("failed to start Mock Signer with error: %v", err)
	}

	t.Cleanup(m.CleanUp)
	if err := m.waitForMockServerToStart(); err != nil {
		t.Fatalf("failed to start Mock Signer with error %v", err)
	}

	return m
}

// GetSupportedKeys 返回当前支持的密钥映射。返回的 map 由 MockSigner 持有，调用方不应对其原地修改。
func (m *MockSigner) GetSupportedKeys() map[string]KeyT {
	m.supportedKeysLock.RLock()
	defer m.supportedKeysLock.RUnlock()
	return m.supportedKeys
}

// SetSupportedKeys 替换 FetchKeys 返回的密钥集合。
func (m *MockSigner) SetSupportedKeys(keys map[string]KeyT) {
	m.supportedKeysLock.Lock()
	defer m.supportedKeysLock.Unlock()
	m.supportedKeys = keys
}

// WaitForSupportedKeysFetch 阻塞等待一次 FetchKeys 完成；若从未调用 FetchKeys，则会一直等待。
func (m *MockSigner) WaitForSupportedKeysFetch() {
	m.supportedKeysLock.Lock()
	defer m.supportedKeysLock.Unlock()
	m.supportedKeysFetched.Wait()
}

// Sign 使用配置的签名密钥和算法签署请求中的 claims，并返回编码后的 header 与签名。
func (m *MockSigner) Sign(ctx context.Context, req *v1.SignJWTRequest) (*v1.SignJWTResponse, error) {

	header := &struct {
		Algorithm string `json:"alg,omitempty"`
		KeyID     string `json:"kid,omitempty"`
		Type      string `json:"typ,omitempty"`
	}{
		Type:      m.TokenType,
		Algorithm: m.SigningAlg,
		KeyID:     m.SigningKeyID,
	}

	headerJSON, err := json.Marshal(header)
	if err != nil {
		return nil, fmt.Errorf("failed to create header for JWT response")
	}

	base64Header := base64.RawURLEncoding.EncodeToString(headerJSON)

	toBeSignedHash := hashBytes([]byte(base64Header + "." + req.Claims))

	signature, err := rsa.SignPKCS1v15(rand.Reader, m.SigningKey, crypto.SHA256, toBeSignedHash)
	if err != nil {
		return nil, fmt.Errorf("unable to sign payload: %w", err)
	}

	return &v1.SignJWTResponse{
		Header:    base64Header,
		Signature: base64.RawURLEncoding.EncodeToString(signature),
	}, nil
}

// FetchKeys 返回当前公钥快照并通知等待的测试；FetchError 非 nil 时直接返回该错误。
func (m *MockSigner) FetchKeys(ctx context.Context, req *v1.FetchKeysRequest) (*v1.FetchKeysResponse, error) {
	m.errorLock.RLocker().Lock()
	defer m.errorLock.RLocker().Unlock()
	if m.FetchError != nil {
		return nil, m.FetchError
	}

	keys := []*v1.Key{}

	m.supportedKeysLock.RLock()
	for id, k := range m.supportedKeys {
		keys = append(keys, &v1.Key{
			KeyId:                    id,
			Key:                      k.Key,
			ExcludeFromOidcDiscovery: k.ExcludeFromOidcDiscovery,
		})
	}
	m.supportedKeysFetched.Broadcast()
	m.supportedKeysLock.RUnlock()

	now := time.Now()
	return &v1.FetchKeysResponse{
		RefreshHintSeconds: 5,
		DataTimestamp:      &timestamppb.Timestamp{Seconds: now.Unix(), Nanos: int32(now.Nanosecond())},
		Keys:               keys,
	}, nil
}

// Metadata 返回配置的最大 Token 有效期；MetadataError 非 nil 时直接返回该错误。
func (m *MockSigner) Metadata(ctx context.Context, req *v1.MetadataRequest) (*v1.MetadataResponse, error) {
	m.errorLock.RLocker().Lock()
	defer m.errorLock.RLocker().Unlock()
	if m.MetadataError != nil {
		return nil, m.MetadataError
	}
	return &v1.MetadataResponse{
		MaxTokenExpirationSeconds: m.MaxTokenExpirationSeconds,
	}, nil
}

// Reset 生成新的签名密钥和三个受支持公钥，并清除故障注入及过期配置。
func (m *MockSigner) Reset() error {

	priv1, pub1, err := generateKeyPair()
	if err != nil {
		return err
	}

	_, pub2, err := generateKeyPair()
	if err != nil {
		return err
	}

	_, pub3, err := generateKeyPair()
	if err != nil {
		return err
	}

	m.SigningKey = priv1
	m.SigningKeyID = "kid-1"
	m.SigningAlg = "RS256"
	m.TokenType = "JWT"
	m.SetSupportedKeys(map[string]KeyT{
		"kid-1": {Key: pub1},
		"kid-2": {Key: pub2},
		"kid-3": {Key: pub3},
	})
	m.errorLock.Lock()
	defer m.errorLock.Unlock()
	m.FetchError = nil
	m.MetadataError = nil
	m.MaxTokenExpirationSeconds = 10 * 60 // 10m

	return nil
}

// start 让模拟 gRPC 服务监听 Unix Socket。
func (m *MockSigner) start(t *testing.T) error {
	var err error

	m.listener, err = net.Listen("unix", m.socketPath)
	if err != nil {
		return fmt.Errorf("failed to listen on the unix socket, error: %w", err)
	}

	klog.Infof("Starting Mock Signer at socketPath %s", m.socketPath)
	go func() {
		if err := m.server.Serve(m.listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Error(err)
		}
	}()
	klog.Infof("Mock Signer listening at socketPath %s", m.socketPath)

	return nil
}

// waitForMockServerToStart 等待模拟服务可用，最多重试 30 秒，超时或仍失败则返回错误。
func (m *MockSigner) waitForMockServerToStart() error {
	var gRPCErr error

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	doneCh := ctx.Done()

	for range 30 {
		select {
		case <-doneCh:
			return fmt.Errorf("failed to start Mock signer: %w", ctx.Err())
		default:
		}
		if _, gRPCErr = m.FetchKeys(context.Background(), &v1.FetchKeysRequest{}); gRPCErr == nil {
			break
		}
		time.Sleep(time.Second)
	}

	if gRPCErr != nil {
		return fmt.Errorf("failed to start Mock signer, gRPC error: %w", gRPCErr)
	}

	return nil
}

// CleanUp 停止 gRPC 服务、关闭监听器并删除 Socket 文件。
func (m *MockSigner) CleanUp() {
	m.server.GracefulStop()
	_ = m.listener.Close()
	_ = os.Remove(m.socketPath)
}

func generateKeyPair() (*rsa.PrivateKey, []byte, error) {

	// 生成新的私钥。
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		klog.Errorf("Error generating private key: %v", err)
		return nil, nil, err
	}

	publicKeyBytes, err := x509.MarshalPKIXPublicKey(&privateKey.PublicKey)
	if err != nil {
		klog.Errorf("Error marshaling public key: %v", err)
		return nil, nil, err
	}

	return privateKey, publicKeyBytes, nil
}

func hashBytes(bytes []byte) []byte {
	hasher := crypto.SHA256.New()
	hasher.Write(bytes)
	return hasher.Sum(nil)
}
