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

package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/storage/storagebackend"
)

// EtcdObjectReader 提供对 etcd 中存储的自定义资源对象的直接访问。
type EtcdObjectReader struct {
	etcdClient    *clientv3.Client
	storagePrefix string
	crd           *apiextensionsv1.CustomResourceDefinition
}

// NewEtcdObjectReader 创建直接从 etcd 读取自定义资源对象的读取器。
func NewEtcdObjectReader(etcdClient *clientv3.Client, restOptions *generic.RESTOptions, crd *apiextensionsv1.CustomResourceDefinition) *EtcdObjectReader {
	return &EtcdObjectReader{etcdClient, restOptions.StorageConfig.Prefix, crd}
}

// WaitForStorageVersion 周期性地调用 updateObjFn，等待 etcd 中存储的自定义资源版本被设置为给定版本。
// updateObjFn 通常应对对象执行一次 noop 更新，这样当 CRD 的存储版本变更后，对象会以新的存储版本写入。
// 超时则返回错误。
// 在更新已有 CRD 的存储版本时很有用，因为该更新不会立即生效。
func (s *EtcdObjectReader) WaitForStorageVersion(version string, ns, name string, timeout time.Duration, updateObjFn func()) error {
	waitCh := time.After(timeout)
	for {
		storage, err := s.GetStoredCustomResource(ns, name)
		if err != nil {
			return err
		}
		if storage.GetObjectKind().GroupVersionKind().Version == version {
			return nil
		}
		select {
		case <-waitCh:
			return fmt.Errorf("timed out after %v waiting for storage version to be %s for object (namespace:%s name:%s)", timeout, version, ns, name)
		case <-time.After(10 * time.Millisecond):
			updateObjFn()
		}
	}
}

// GetStoredCustomResource 从 etcd 获取自定义资源的存储形式。
func (s *EtcdObjectReader) GetStoredCustomResource(ns, name string) (*unstructured.Unstructured, error) {
	key := path.Join("/", s.storagePrefix, s.crd.Spec.Group, s.crd.Spec.Names.Plural, ns, name)
	resp, err := s.etcdClient.KV.Get(context.Background(), key)
	if err != nil {
		return nil, fmt.Errorf("error getting storage object %s, %s from etcd at key %s: %v", ns, name, key, err)
	}
	if len(resp.Kvs) == 0 {
		return nil, fmt.Errorf("no storage object found for %s, %s in etcd for key %s", ns, name, key)
	}
	raw := resp.Kvs[0].Value
	u := &unstructured.Unstructured{Object: map[string]interface{}{}}
	if err := json.Unmarshal(raw, u); err != nil {
		return nil, fmt.Errorf("error deserializing object %s: %v", string(raw), err)
	}
	return u, nil
}

// SetStoredCustomResource 将自定义资源的存储形式写入 etcd。
func (s *EtcdObjectReader) SetStoredCustomResource(ns, name string, obj *unstructured.Unstructured) error {
	bs, err := obj.MarshalJSON()
	if err != nil {
		return err
	}

	key := path.Join("/", s.storagePrefix, s.crd.Spec.Group, s.crd.Spec.Names.Plural, ns, name)
	if _, err := s.etcdClient.KV.Put(context.Background(), key, string(bs)); err != nil {
		return fmt.Errorf("error setting storage object %s, %s from etcd at key %s: %v", ns, name, key, err)
	}
	return nil
}

// GetEtcdClients 返回已初始化的 clientv3.Client 和 clientv3.KV。
func GetEtcdClients(config storagebackend.TransportConfig) (*clientv3.Client, clientv3.KV, error) {
	tlsInfo := transport.TLSInfo{
		CertFile:      config.CertFile,
		KeyFile:       config.KeyFile,
		TrustedCAFile: config.TrustedCAFile,
	}

	tlsConfig, err := tlsInfo.ClientConfig()
	if err != nil {
		return nil, nil, err
	}

	cfg := clientv3.Config{
		Endpoints:   config.ServerList,
		DialTimeout: 20 * time.Second,
		TLS:         tlsConfig,
	}

	c, err := clientv3.New(cfg)
	if err != nil {
		return nil, nil, err
	}

	return c, clientv3.NewKV(c), nil
}

// SetStoredCustomResourceDefinition 将 CRD 的存储形式写入 etcd。
func (s *EtcdObjectReader) SetStoredCustomResourceDefinition(name string, crd *apiextensionsv1.CustomResourceDefinition) error {
	bs, err := json.Marshal(crd)
	if err != nil {
		return err
	}
	key := path.Join("/", s.storagePrefix, "apiextensions.k8s.io", "customresourcedefinitions", name)
	if _, err := s.etcdClient.Put(context.Background(), key, string(bs)); err != nil {
		return fmt.Errorf("error setting CRD %s in etcd at key %s: %w", name, key, err)
	}
	return nil
}
