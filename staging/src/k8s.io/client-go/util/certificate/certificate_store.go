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

package certificate

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"time"

	certutil "k8s.io/client-go/util/cert"
	"k8s.io/klog/v2"
)

const (
	keyExtension  = ".key"
	certExtension = ".crt"
	pemExtension  = ".pem"
	// current/updated 是轮转时用于原子切换 symlink 的两个固定名字。
	currentPair = "current"
	updatedPair = "updated"
)

// fileStore 是 FileStore 的基于文件系统的实现。
type fileStore struct {
	logger         klog.Logger
	pairNamePrefix string
	certDirectory  string
	keyDirectory   string
	certFile       string
	keyFile        string
}

// FileStore is a store that provides certificate retrieval as well as
// the path on disk of the current PEM.
//
// FileStore 在 Store 之外额外暴露当前证书在磁盘上的 PEM 路径。
type FileStore interface {
	Store
	// CurrentPath returns the path on disk of the current certificate/key
	// pair encoded as PEM files.
	//
	// CurrentPath 返回当前证书/私钥对（PEM）在磁盘上的路径。
	CurrentPath() string
}

// NewFileStore returns a concrete implementation of a Store that is based on
// storing the cert/key pairs in a single file per pair on disk in the
// designated directory. When starting up it will look for the currently
// selected cert/key pair in:
//
// 1. ${certDirectory}/${pairNamePrefix}-current.pem - both cert and key are in the same file.
// 2. ${certFile}, ${keyFile}
// 3. ${certDirectory}/${pairNamePrefix}.crt, ${keyDirectory}/${pairNamePrefix}.key
//
// The first one found will be used. If rotation is enabled, future cert/key
// updates will be written to the ${certDirectory} directory and
// ${certDirectory}/${pairNamePrefix}-current.pem will be created as a soft
// link to the currently selected cert/key pair.
//
// Contextual logging: NewFileStoreWithLogger should be used instead of NewFileStore in code which supports contextual logging.
//
// NewFileStore 创建基于文件系统的证书存储。启动时按上述顺序查找已存在的证书/私钥对，
// 使用第一个找到的；启用轮转后，新证书写入 certDirectory，并更新 current 符号链接。
func NewFileStore(
	pairNamePrefix string,
	certDirectory string,
	keyDirectory string,
	certFile string,
	keyFile string) (FileStore, error) {
	return NewFileStoreWithLogger(klog.Background(), pairNamePrefix, certDirectory, keyDirectory, certFile, keyFile)
}

// NewFileStoreWithLogger is a variant of NewFileStore where the caller is in
// control of logging. All log messages get emitted with logger.Info, so
// pass e.g. logger.V(3) to make logging less verbose.
//
// NewFileStoreWithLogger 是允许调用方控制日志的 NewFileStore 变体。
func NewFileStoreWithLogger(
	logger klog.Logger,
	pairNamePrefix string,
	certDirectory string,
	keyDirectory string,
	certFile string,
	keyFile string) (FileStore, error) {

	s := fileStore{
		logger:         logger,
		pairNamePrefix: pairNamePrefix,
		certDirectory:  certDirectory,
		keyDirectory:   keyDirectory,
		certFile:       certFile,
		keyFile:        keyFile,
	}
	// 启动时先修复可能被中断的轮转，避免后续读写落在不一致的状态上。
	if err := s.recover(); err != nil {
		return nil, err
	}
	return &s, nil
}

// CurrentPath returns the path to the current version of these certificates.
func (s *fileStore) CurrentPath() string {
	return filepath.Join(s.certDirectory, s.filename(currentPair))
}

// recover checks if there is a certificate rotation that was interrupted while
// progress, and if so, attempts to recover to a good state.
//
// recover 处理轮转中途崩溃的情况：若 current 不存在但 updated 符号链接存在，
// 说明轮转在最后一步改名之前中断，将其重命名为 current 完成切换。
func (s *fileStore) recover() error {
	// If the 'current' file doesn't exist, continue on with the recovery process.
	currentPath := filepath.Join(s.certDirectory, s.filename(currentPair))
	if exists, err := fileExists(currentPath); err != nil {
		return err
	} else if exists {
		return nil
	}

	// If the 'updated' file exists, and it is a symbolic link, continue on
	// with the recovery process.
	updatedPath := filepath.Join(s.certDirectory, s.filename(updatedPair))
	if fi, err := os.Lstat(updatedPath); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	} else if fi.Mode()&os.ModeSymlink != os.ModeSymlink {
		// updated 必须是符号链接，普通文件说明状态被外部破坏，无法安全恢复。
		return fmt.Errorf("expected %q to be a symlink but it is a file", updatedPath)
	}

	// Move the 'updated' symlink to 'current'.
	if err := os.Rename(updatedPath, currentPath); err != nil {
		return fmt.Errorf("unable to rename %q to %q: %v", updatedPath, currentPath, err)
	}
	return nil
}

// Current 按 NewFileStore 描述的优先级加载当前证书/私钥对。
func (s *fileStore) Current() (*tls.Certificate, error) {
	pairFile := filepath.Join(s.certDirectory, s.filename(currentPair))
	if pairFileExists, err := fileExists(pairFile); err != nil {
		return nil, err
	} else if pairFileExists {
		s.logger.Info("Loading cert/key pair from a file", "filePath", pairFile)
		return loadFile(pairFile)
	}

	certFileExists, err := fileExists(s.certFile)
	if err != nil {
		return nil, err
	}
	keyFileExists, err := fileExists(s.keyFile)
	if err != nil {
		return nil, err
	}
	if certFileExists && keyFileExists {
		s.logger.Info("Loading cert/key pair", "certFile", s.certFile, "keyFile", s.keyFile)
		return loadX509KeyPair(s.certFile, s.keyFile)
	}

	c := filepath.Join(s.certDirectory, s.pairNamePrefix+certExtension)
	k := filepath.Join(s.keyDirectory, s.pairNamePrefix+keyExtension)
	certFileExists, err = fileExists(c)
	if err != nil {
		return nil, err
	}
	keyFileExists, err = fileExists(k)
	if err != nil {
		return nil, err
	}
	if certFileExists && keyFileExists {
		s.logger.Info("Loading cert/key pair", "certFile", c, "keyFile", k)
		return loadX509KeyPair(c, k)
	}

	noKeyErr := NoCertKeyError(
		fmt.Sprintf("no cert/key files read at %q, (%q, %q) or (%q, %q)",
			pairFile,
			s.certFile,
			s.keyFile,
			s.certDirectory,
			s.keyDirectory))
	return nil, &noKeyErr
}

// loadFile 从单个 PEM 文件加载证书与私钥，并填充 Leaf 缓存解析结果。
func loadFile(pairFile string) (*tls.Certificate, error) {
	// LoadX509KeyPair knows how to parse combined cert and private key from
	// the same file.
	cert, err := tls.LoadX509KeyPair(pairFile, pairFile)
	if err != nil {
		return nil, fmt.Errorf("could not convert data from %q into cert/key pair: %v", pairFile, err)
	}
	certs, err := x509.ParseCertificates(cert.Certificate[0])
	if err != nil {
		return nil, fmt.Errorf("unable to parse certificate data: %v", err)
	}
	cert.Leaf = certs[0]
	return &cert, nil
}

// Update 将新的证书/私钥对写入带时间戳的文件，并把 current 符号链接指向它。
func (s *fileStore) Update(certData, keyData []byte) (*tls.Certificate, error) {
	ts := time.Now().Format("2006-01-02-15-04-05")
	pemFilename := s.filename(ts)

	if err := os.MkdirAll(s.certDirectory, 0755); err != nil {
		return nil, fmt.Errorf("could not create directory %q to store certificates: %v", s.certDirectory, err)
	}
	certPath := filepath.Join(s.certDirectory, pemFilename)

	// 私钥与证书合并在同一文件，权限限定为 0600，防止私钥被其他用户读取。
	f, err := os.OpenFile(certPath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("could not open %q: %v", certPath, err)
	}
	defer f.Close()

	// First cert is leaf, remainder are intermediates
	certs, err := certutil.ParseCertsPEM(certData)
	if err != nil {
		return nil, fmt.Errorf("invalid certificate data: %v", err)
	}
	for _, c := range certs {
		pem.Encode(f, &pem.Block{Type: "CERTIFICATE", Bytes: c.Raw})
	}

	keyBlock, _ := pem.Decode(keyData)
	if keyBlock == nil {
		return nil, fmt.Errorf("invalid key data")
	}
	pem.Encode(f, keyBlock)

	// Ensure data is written to disk
	// 落盘后再切换符号链接，避免指向尚未持久化的文件。
	if err := f.Sync(); err != nil {
		return nil, fmt.Errorf("failed to sync certificate data to disk: %w (file: %q)", err, certPath)
	}

	cert, err := loadFile(certPath)
	if err != nil {
		return nil, err
	}

	if err := s.updateSymlink(certPath); err != nil {
		return nil, err
	}
	return cert, nil
}

// updateSymLink updates the current symlink to point to the file that is
// passed it. It will fail if there is a non-symlink file exists where the
// symlink is expected to be.
//
// updateSymlink 通过“先建 updated 再 rename”的方式原子替换 current 符号链接；
// 若 target 不是符号链接则报错，防止覆盖用户手工放置的文件。
func (s *fileStore) updateSymlink(filename string) error {
	// If the 'current' file either doesn't exist, or is already a symlink,
	// proceed. Otherwise, this is an unrecoverable error.
	currentPath := filepath.Join(s.certDirectory, s.filename(currentPair))
	currentPathExists := false
	if fi, err := os.Lstat(currentPath); err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	} else if fi.Mode()&os.ModeSymlink != os.ModeSymlink {
		return fmt.Errorf("expected %q to be a symlink but it is a file", currentPath)
	} else {
		currentPathExists = true
	}

	// If the 'updated' file doesn't exist, proceed. If it exists but it is a
	// symlink, delete it.  Otherwise, this is an unrecoverable error.
	updatedPath := filepath.Join(s.certDirectory, s.filename(updatedPair))
	if fi, err := os.Lstat(updatedPath); err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	} else if fi.Mode()&os.ModeSymlink != os.ModeSymlink {
		return fmt.Errorf("expected %q to be a symlink but it is a file", updatedPath)
	} else {
		// 残留的 updated 符号链接来自上次中断的轮转，删除后重建。
		if err := os.Remove(updatedPath); err != nil {
			return fmt.Errorf("unable to remove %q: %v", updatedPath, err)
		}
	}

	// Check that the new cert/key pair file exists to avoid rotating to an
	// invalid cert/key.
	if filenameExists, err := fileExists(filename); err != nil {
		return err
	} else if !filenameExists {
		return fmt.Errorf("file %q does not exist so it can not be used as the currently selected cert/key", filename)
	}

	// Ensure the source path is absolute to ensure the symlink target is
	// correct when certDirectory is a relative path.
	filename, err := filepath.Abs(filename)
	if err != nil {
		return err
	}

	// Create the 'updated' symlink pointing to the requested file name.
	if err := os.Symlink(filename, updatedPath); err != nil {
		return fmt.Errorf("unable to create a symlink from %q to %q: %v", updatedPath, filename, err)
	}

	// Replace the 'current' symlink.
	if currentPathExists {
		if err := os.Remove(currentPath); err != nil {
			return fmt.Errorf("unable to remove %q: %v", currentPath, err)
		}
	}
	if err := os.Rename(updatedPath, currentPath); err != nil {
		return fmt.Errorf("unable to rename %q to %q: %v", updatedPath, currentPath, err)
	}
	return nil
}

// filename 生成 "<prefix>-<qualifier>.pem" 形式的文件名。
func (s *fileStore) filename(qualifier string) string {
	return s.pairNamePrefix + "-" + qualifier + pemExtension
}

// loadX509KeyPair 从分开的 cert/key 文件加载证书对，并填充 Leaf。
func loadX509KeyPair(certFile, keyFile string) (*tls.Certificate, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	certs, err := x509.ParseCertificates(cert.Certificate[0])
	if err != nil {
		return nil, fmt.Errorf("unable to parse certificate data: %v", err)
	}
	cert.Leaf = certs[0]
	return &cert, nil
}

// FileExists checks if specified file exists.
//
// fileExists 判断文件是否存在；不存在返回 (false, nil)，其他错误原样返回。
func fileExists(filename string) (bool, error) {
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, nil
}
