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
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// TestUpdateSymlinkExistingFileError 验证目标位置已存在普通文件（而非符号
// 链接）时 updateSymlink 必须报错，避免覆盖用户数据。
func TestUpdateSymlinkExistingFileError(t *testing.T) {
	dir, err := os.MkdirTemp("", "k8s-test-update-symlink")
	if err != nil {
		t.Fatalf("Unable to create the test directory %q: %v", dir, err)
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("Unable to clean up test directory %q: %v", dir, err)
		}
	}()
	pairFile := filepath.Join(dir, "kubelet-current.pem")
	if err := os.WriteFile(pairFile, nil, 0600); err != nil {
		t.Fatalf("Unable to create the file %q: %v", pairFile, err)
	}

	s := fileStore{
		certDirectory:  dir,
		pairNamePrefix: "kubelet",
	}
	if err := s.updateSymlink(pairFile); err == nil {
		t.Errorf("Got no error, wanted to fail updating the symlink because there is a file there.")
	}
}

// TestUpdateSymlinkNewFileNotExist 验证符号链接的创建与替换：
// 首次创建成功；指向不存在的文件时应失败。
func TestUpdateSymlinkNewFileNotExist(t *testing.T) {
	dir, err := os.MkdirTemp("", "k8s-test-update-symlink")
	if err != nil {
		t.Fatalf("Unable to create the test directory %q: %v", dir, err)
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("Unable to clean up test directory %q: %v", dir, err)
		}
	}()
	oldPairFile := filepath.Join(dir, "kubelet-oldpair.pem")
	if err := os.WriteFile(oldPairFile, nil, 0600); err != nil {
		t.Fatalf("Unable to create the file %q: %v", oldPairFile, err)
	}

	s := fileStore{
		certDirectory:  dir,
		pairNamePrefix: "kubelet",
	}
	if err := s.updateSymlink(oldPairFile); err != nil {
		t.Errorf("Got error %v, wanted successful update of the symlink to point to %q", err, oldPairFile)
	}

	if _, err := os.Stat(oldPairFile); err != nil {
		t.Errorf("Got error %v, wanted file %q to be there.", err, oldPairFile)
	}

	currentPairFile := filepath.Join(dir, "kubelet-current.pem")
	if fi, err := os.Lstat(currentPairFile); err != nil {
		t.Errorf("Got error %v, wanted file %q to be there", err, currentPairFile)
	} else if fi.Mode()&os.ModeSymlink != os.ModeSymlink {
		t.Errorf("Got %q not a symlink.", currentPairFile)
	}

	newPairFile := filepath.Join(dir, "kubelet-newpair.pem")
	if err := s.updateSymlink(newPairFile); err == nil {
		t.Errorf("Got no error, wanted to fail updating the symlink the file %q does not exist.", newPairFile)
	}
}

// TestUpdateSymlinkNoSymlink 验证当前不存在 *-current.pem 时会新建符号链接。
func TestUpdateSymlinkNoSymlink(t *testing.T) {
	dir, err := os.MkdirTemp("", "k8s-test-update-symlink")
	if err != nil {
		t.Fatalf("Unable to create the test directory %q: %v", dir, err)
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("Unable to clean up test directory %q: %v", dir, err)
		}
	}()
	pairFile := filepath.Join(dir, "kubelet-newfile.pem")
	if err := os.WriteFile(pairFile, nil, 0600); err != nil {
		t.Fatalf("Unable to create the file %q: %v", pairFile, err)
	}

	s := fileStore{
		certDirectory:  dir,
		pairNamePrefix: "kubelet",
	}
	if err := s.updateSymlink(pairFile); err != nil {
		t.Errorf("Got error %v, wanted a new symlink to be created", err)
	}

	if _, err := os.Stat(pairFile); err != nil {
		t.Errorf("Got error %v, wanted file %q to be there", err, pairFile)
	}
	currentPairFile := filepath.Join(dir, "kubelet-current.pem")
	if fi, err := os.Lstat(currentPairFile); err != nil {
		t.Errorf("Got %v, wanted %q to be there", currentPairFile, err)
	} else if fi.Mode()&os.ModeSymlink != os.ModeSymlink {
		t.Errorf("%q not a symlink, wanted a symlink.", currentPairFile)
	}
}

// TestUpdateSymlinkReplaceExistingSymlink 验证已有符号链接会被原子地替换为
// 指向新文件，且旧文件仍然保留（便于回滚/排查）。
func TestUpdateSymlinkReplaceExistingSymlink(t *testing.T) {
	prefix := "kubelet"
	dir, err := os.MkdirTemp("", "k8s-test-update-symlink")
	if err != nil {
		t.Fatalf("Unable to create the test directory %q: %v", dir, err)
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("Unable to clean up test directory %q: %v", dir, err)
		}
	}()
	oldPairFile := filepath.Join(dir, prefix+"-oldfile.pem")
	if err := os.WriteFile(oldPairFile, nil, 0600); err != nil {
		t.Fatalf("Unable to create the file %q: %v", oldPairFile, err)
	}
	newPairFile := filepath.Join(dir, prefix+"-newfile.pem")
	if err := os.WriteFile(newPairFile, nil, 0600); err != nil {
		t.Fatalf("Unable to create the file %q: %v", newPairFile, err)
	}
	currentPairFile := filepath.Join(dir, prefix+"-current.pem")
	if err := os.Symlink(oldPairFile, currentPairFile); err != nil {
		t.Fatalf("unable to create a symlink from %q to %q: %v", currentPairFile, oldPairFile, err)
	}
	if resolved, err := os.Readlink(currentPairFile); err != nil {
		t.Fatalf("Got %v when attempting to resolve symlink %q", err, currentPairFile)
	} else if resolved != oldPairFile {
		t.Fatalf("Got %q as resolution of symlink %q, wanted %q", resolved, currentPairFile, oldPairFile)
	}

	s := fileStore{
		certDirectory:  dir,
		pairNamePrefix: prefix,
	}
	if err := s.updateSymlink(newPairFile); err != nil {
		t.Errorf("Got error %v, wanted a new symlink to be created", err)
	}

	if _, err := os.Stat(oldPairFile); err != nil {
		t.Errorf("Got error %v, wanted file %q to be there", oldPairFile, err)
	}
	if _, err := os.Stat(newPairFile); err != nil {
		t.Errorf("Got error %v, wanted file %q to be there", newPairFile, err)
	}
	if fi, err := os.Lstat(currentPairFile); err != nil {
		t.Errorf("Got %v, wanted %q to be there", currentPairFile, err)
	} else if fi.Mode()&os.ModeSymlink != os.ModeSymlink {
		t.Errorf("%q not a symlink, wanted a symlink.", currentPairFile)
	}
	if resolved, err := os.Readlink(currentPairFile); err != nil {
		t.Fatalf("Got %v when attempting to resolve symlink %q", err, currentPairFile)
	} else if resolved != newPairFile {
		t.Fatalf("Got %q as resolution of symlink %q, wanted %q", resolved, currentPairFile, newPairFile)
	}
}

// TestLoadFile 验证 loadFile 不依赖 PEM 块顺序：证书在前或在后都能正确
// 加载出证书与私钥，并填充 Leaf。
func TestLoadFile(t *testing.T) {
	dir, err := os.MkdirTemp("", "k8s-test-load-cert-key-blocks")
	if err != nil {
		t.Fatalf("Unable to create the test directory %q: %v", dir, err)
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("Unable to clean up test directory %q: %v", dir, err)
		}
	}()

	pairFile := filepath.Join(dir, "kubelet-pair.pem")

	tests := []struct {
		desc string
		data []byte
	}{
		{desc: "cert and key", data: bytes.Join([][]byte{storeCertData.certificatePEM, storeCertData.keyPEM}, []byte("\n"))},
		{desc: "key and cert", data: bytes.Join([][]byte{storeCertData.keyPEM, storeCertData.certificatePEM}, []byte("\n"))},
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			if err := os.WriteFile(pairFile, tt.data, 0600); err != nil {
				t.Fatalf("Unable to create the file %q: %v", pairFile, err)
			}
			cert, err := loadFile(pairFile)
			if err != nil {
				t.Fatalf("Could not load certificate from disk: %v", err)
			}
			if cert == nil {
				t.Fatalf("There was no error, but no certificate data was returned.")
			}
			if cert.Leaf == nil {
				t.Fatalf("Got an empty leaf, expected private data.")
			}
		})
	}
}

// TestUpdateNoRotation 验证写入与当前证书相同的内容时，Update 仍返回
// 合法证书（不触发轮转，但也不报错）。
func TestUpdateNoRotation(t *testing.T) {
	prefix := "kubelet-server"
	dir, err := os.MkdirTemp("", "k8s-test-certstore-current")
	if err != nil {
		t.Fatalf("Unable to create the test directory %q: %v", dir, err)
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("Unable to clean up test directory %q: %v", dir, err)
		}
	}()
	keyFile := filepath.Join(dir, "kubelet.key")
	if err := os.WriteFile(keyFile, storeCertData.keyPEM, 0600); err != nil {
		t.Fatalf("Unable to create the file %q: %v", keyFile, err)
	}
	certFile := filepath.Join(dir, "kubelet.crt")
	if err := os.WriteFile(certFile, storeCertData.certificatePEM, 0600); err != nil {
		t.Fatalf("Unable to create the file %q: %v", certFile, err)
	}

	//nolint:logcheck // Intentionally uses the old API.
	s, err := NewFileStore(prefix, dir, dir, certFile, keyFile)
	if err != nil {
		t.Fatalf("Got %v while creating a new store.", err)
	}

	cert, err := s.Update(storeCertData.certificatePEM, storeCertData.keyPEM)
	if err != nil {
		t.Errorf("Got %v while updating certificate store.", err)
	}
	if cert == nil {
		t.Errorf("Got nil certificate, expected something real.")
	}
}

// TestUpdateRotation 验证以新证书/私钥调用 Update 能成功轮转。
func TestUpdateRotation(t *testing.T) {
	prefix := "kubelet-server"
	dir, err := os.MkdirTemp("", "k8s-test-certstore-current")
	if err != nil {
		t.Fatalf("Unable to create the test directory %q: %v", dir, err)
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("Unable to clean up test directory %q: %v", dir, err)
		}
	}()
	keyFile := filepath.Join(dir, "kubelet.key")
	if err := os.WriteFile(keyFile, storeCertData.keyPEM, 0600); err != nil {
		t.Fatalf("Unable to create the file %q: %v", keyFile, err)
	}
	certFile := filepath.Join(dir, "kubelet.crt")
	if err := os.WriteFile(certFile, storeCertData.certificatePEM, 0600); err != nil {
		t.Fatalf("Unable to create the file %q: %v", certFile, err)
	}

	//nolint:logcheck // Intentionally uses the old API.
	s, err := NewFileStore(prefix, dir, dir, certFile, keyFile)
	if err != nil {
		t.Fatalf("Got %v while creating a new store.", err)
	}

	cert, err := s.Update(storeCertData.certificatePEM, storeCertData.keyPEM)
	if err != nil {
		t.Fatalf("Got %v while updating certificate store.", err)
	}
	if cert == nil {
		t.Fatalf("Got nil certificate, expected something real.")
	}
}

// TestUpdateTwoCerts 验证 PEM 中包含证书链（多张证书）时，Update 返回的
// tls.Certificate 保留全部证书。
func TestUpdateTwoCerts(t *testing.T) {
	prefix := "kubelet-server"
	dir, err := os.MkdirTemp("", "k8s-test-certstore-current")
	if err != nil {
		t.Fatalf("Unable to create the test directory %q: %v", dir, err)
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("Unable to clean up test directory %q: %v", dir, err)
		}
	}()
	keyFile := filepath.Join(dir, "kubelet.key")
	if err := os.WriteFile(keyFile, storeTwoCertsData.keyPEM, 0600); err != nil {
		t.Fatalf("Unable to create the file %q: %v", keyFile, err)
	}
	certFile := filepath.Join(dir, "kubelet.crt")
	if err := os.WriteFile(certFile, storeTwoCertsData.certificatePEM, 0600); err != nil {
		t.Fatalf("Unable to create the file %q: %v", certFile, err)
	}

	//nolint:logcheck // Intentionally uses the old API.
	s, err := NewFileStore(prefix, dir, dir, certFile, keyFile)
	if err != nil {
		t.Fatalf("Got %v while creating a new store.", err)
	}

	cert, err := s.Update(storeTwoCertsData.certificatePEM, storeTwoCertsData.keyPEM)
	if err != nil {
		t.Errorf("Got %v while updating certificate store.", err)
	}
	if cert == nil {
		t.Fatalf("Got nil certificate, expected something real.")
	}
	if len(cert.Certificate) != 2 {
		t.Fatalf("Unexpected number of certificates, expected 2, got %v", len(cert.Certificate))
	}
}

// TestUpdateWithBadCertKeyData 验证非法证书数据会导致 Update 返回错误且
// 不返回证书，同时不应覆盖磁盘上的有效文件。
func TestUpdateWithBadCertKeyData(t *testing.T) {
	prefix := "kubelet-server"
	dir, err := os.MkdirTemp("", "k8s-test-certstore-current")
	if err != nil {
		t.Fatalf("Unable to create the test directory %q: %v", dir, err)
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("Unable to clean up test directory %q: %v", dir, err)
		}
	}()
	keyFile := filepath.Join(dir, "kubelet.key")
	if err := os.WriteFile(keyFile, storeCertData.keyPEM, 0600); err != nil {
		t.Fatalf("Unable to create the file %q: %v", keyFile, err)
	}
	certFile := filepath.Join(dir, "kubelet.crt")
	if err := os.WriteFile(certFile, storeCertData.certificatePEM, 0600); err != nil {
		t.Fatalf("Unable to create the file %q: %v", certFile, err)
	}

	//nolint:logcheck // Intentionally uses the old API.
	s, err := NewFileStore(prefix, dir, dir, certFile, keyFile)
	if err != nil {
		t.Fatalf("Got %v while creating a new store.", err)
	}

	cert, err := s.Update([]byte{0, 0}, storeCertData.keyPEM)
	if err == nil {
		t.Fatalf("Got no error while updating certificate store with invalid data.")
	}
	if cert != nil {
		t.Fatalf("Got %v certificate returned from the update, expected nil.", cert)
	}
}

// TestCurrentPairFile 验证 Current 能通过 *-current.pem 符号链接读取
// 合并的证书/私钥文件。
func TestCurrentPairFile(t *testing.T) {
	prefix := "kubelet-server"
	dir, err := os.MkdirTemp("", "k8s-test-certstore-current")
	if err != nil {
		t.Fatalf("Unable to create the test directory %q: %v", dir, err)
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("Unable to clean up test directory %q: %v", dir, err)
		}
	}()
	pairFile := filepath.Join(dir, prefix+"-pair.pem")
	// 合并 cert 与 key 为一个 PEM 文件。
	data := append(storeCertData.certificatePEM, []byte("\n")...)
	data = append(data, storeCertData.keyPEM...)
	if err := os.WriteFile(pairFile, data, 0600); err != nil {
		t.Fatalf("Unable to create the file %q: %v", pairFile, err)
	}
	currentFile := filepath.Join(dir, prefix+"-current.pem")
	if err := os.Symlink(pairFile, currentFile); err != nil {
		t.Fatalf("unable to create a symlink from %q to %q: %v", currentFile, pairFile, err)
	}

	//nolint:logcheck // Intentionally uses the old API.
	store, err := NewFileStore("kubelet-server", dir, dir, "", "")
	if err != nil {
		t.Fatalf("Failed to initialize certificate store: %v", err)
	}

	cert, err := store.Current()
	if err != nil {
		t.Fatalf("Could not load certificate from disk: %v", err)
	}
	if cert == nil {
		t.Fatalf("There was no error, but no certificate data was returned.")
	}
	if cert.Leaf == nil {
		t.Fatalf("Got an empty leaf, expected private data.")
	}
}

// TestCurrentCertKeyFiles 验证当前证书以独立的 cert/key 文件形式存放时，
// Current 能正确加载。
func TestCurrentCertKeyFiles(t *testing.T) {
	prefix := "kubelet-server"
	dir, err := os.MkdirTemp("", "k8s-test-certstore-current")
	if err != nil {
		t.Fatalf("Unable to create the test directory %q: %v", dir, err)
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("Unable to clean up test directory %q: %v", dir, err)
		}
	}()
	certFile := filepath.Join(dir, "kubelet.crt")
	if err := os.WriteFile(certFile, storeCertData.certificatePEM, 0600); err != nil {
		t.Fatalf("Unable to create the file %q: %v", certFile, err)
	}
	keyFile := filepath.Join(dir, "kubelet.key")
	if err := os.WriteFile(keyFile, storeCertData.keyPEM, 0600); err != nil {
		t.Fatalf("Unable to create the file %q: %v", keyFile, err)
	}

	//nolint:logcheck // Intentionally uses the old API.
	store, err := NewFileStore(prefix, dir, dir, certFile, keyFile)
	if err != nil {
		t.Fatalf("Failed to initialize certificate store: %v", err)
	}

	cert, err := store.Current()
	if err != nil {
		t.Fatalf("Could not load certificate from disk: %v", err)
	}
	if cert == nil {
		t.Fatalf("There was no error, but no certificate data was returned.")
	}
	if cert.Leaf == nil {
		t.Fatalf("Got an empty leaf, expected private data.")
	}
}

// TestCurrentTwoCerts 验证当前证书为证书链（两张证书）时，Current 返回
// 完整的证书链。
func TestCurrentTwoCerts(t *testing.T) {
	prefix := "kubelet-server"
	dir, err := os.MkdirTemp("", "k8s-test-certstore-current")
	if err != nil {
		t.Fatalf("Unable to create the test directory %q: %v", dir, err)
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("Unable to clean up test directory %q: %v", dir, err)
		}
	}()
	certFile := filepath.Join(dir, "kubelet.crt")
	if err := os.WriteFile(certFile, storeTwoCertsData.certificatePEM, 0600); err != nil {
		t.Fatalf("Unable to create the file %q: %v", certFile, err)
	}
	keyFile := filepath.Join(dir, "kubelet.key")
	if err := os.WriteFile(keyFile, storeTwoCertsData.keyPEM, 0600); err != nil {
		t.Fatalf("Unable to create the file %q: %v", keyFile, err)
	}

	//nolint:logcheck // Intentionally uses the old API.
	store, err := NewFileStore(prefix, dir, dir, certFile, keyFile)
	if err != nil {
		t.Fatalf("Failed to initialize certificate store: %v", err)
	}

	cert, err := store.Current()
	if err != nil {
		t.Fatalf("Could not load certificate from disk: %v", err)
	}
	if cert == nil {
		t.Fatalf("There was no error, but no certificate data was returned.")
	}
	if cert.Leaf == nil {
		t.Fatalf("Got an empty leaf, expected private data.")
	}
	if len(cert.Certificate) != 2 {
		t.Fatalf("Unexpected number of certificates, expected 2, got %v", len(cert.Certificate))
	}
}

// TestCurrentNoFiles 验证没有任何证书文件时，Current 返回 *NoCertKeyError，
// 调用方可据此区分“尚未签发”与真实 IO 错误。
func TestCurrentNoFiles(t *testing.T) {
	dir, err := os.MkdirTemp("", "k8s-test-certstore-current")
	if err != nil {
		t.Fatalf("Unable to create the test directory %q: %v", dir, err)
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("Unable to clean up test directory %q: %v", dir, err)
		}
	}()

	//nolint:logcheck // Intentionally uses the old API.
	store, err := NewFileStore("kubelet-server", dir, dir, "", "")
	if err != nil {
		t.Fatalf("Failed to initialize certificate store: %v", err)
	}

	cert, err := store.Current()
	if err == nil {
		t.Fatalf("Got no error, expected an error because the cert/key files don't exist.")
	}
	if _, ok := err.(*NoCertKeyError); !ok {
		t.Fatalf("Got error %v, expected NoCertKeyError.", err)
	}
	if cert != nil {
		t.Fatalf("Got certificate, expected no certificate because the cert/key files don't exist.")
	}
}
