//go:build linux

/*
Copyright 2021 The Kubernetes Authors.

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

// Package users 包含管理用户的工具。
package users

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"k8s.io/klog/v2"

	"k8s.io/kubernetes/cmd/kubeadm/app/constants"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
)

// EntryMap 保存用户或组条目映射。
type EntryMap struct {
	entries map[string]*entry
}

// UsersAndGroups 保存用户和组条目映射，由 AddUsersAndGroups 返回。
type UsersAndGroups struct {
	// Users 是用户条目映射。
	Users *EntryMap
	// Groups 是组条目映射。
	Groups *EntryMap
}

// entry 保存 UNIX 用户或组信息，可部分兼容解析 /etc/passwd 用户和 /etc/group 组。
type entry struct {
	name      string
	id        int64
	gid       int64
	userNames []string
	shell     string
}

// limits 保存 UID 和 GID 的系统最小、最大范围。
type limits struct {
	minUID, maxUID, minGID, maxGID int64
}

const (
	// 这些常量定义解析 /etc/passwd 或 /etc/group 时每条记录的字段数。
	totalFieldsGroup = 4
	totalFieldsUser  = 7

	// klogLevel 保存输出使用的 klog 级别。
	klogLevel = 5

	// noshell 保存用于禁用 shell 登录的二进制路径。
	noshell = "/bin/false"

	// 这些是 Linux 默认系统路径常量。
	fileEtcLoginDefs = "/etc/login.defs"
	fileEtcPasswd    = "/etc/passwd"
	fileEtcGroup     = "/etc/group"
)

var (
	// 这些条目定义需创建的用户和组，来源：
	// https://git.k8s.io/enhancements/keps/sig-cluster-lifecycle/kubeadm/2568-kubeadm-non-root-control-plane
	usersToCreateSpec = []*entry{
		{name: constants.EtcdUserName},
		{name: constants.KubeAPIServerUserName},
		{name: constants.KubeControllerManagerUserName},
		{name: constants.KubeSchedulerUserName},
	}
	groupsToCreateSpec = []*entry{
		{name: constants.EtcdUserName, userNames: []string{constants.EtcdUserName}},
		{name: constants.KubeAPIServerUserName, userNames: []string{constants.KubeAPIServerUserName}},
		{name: constants.KubeControllerManagerUserName, userNames: []string{constants.KubeControllerManagerUserName}},
		{name: constants.KubeSchedulerUserName, userNames: []string{constants.KubeSchedulerUserName}},
		{name: constants.ServiceAccountKeyReadersGroupName, userNames: []string{constants.KubeAPIServerUserName, constants.KubeControllerManagerUserName}},
	}

	// defaultLimits 保存 /etc/login.defs 缺少值时的默认范围。
	defaultLimits = &limits{minUID: 100, maxUID: 999, minGID: 100, maxGID: 999}
)

// ID 根据条目名称返回 ID：用户条目返回 UID，组条目返回 GID；不存在时返回 nil。
func (u *EntryMap) ID(name string) *int64 {
	entry, ok := u.entries[name]
	if !ok {
		return nil
	}
	id := entry.id
	return &id
}

// String 将 EntryMap 转换为可读字符串。
func (u *EntryMap) String() string {
	lines := make([]string, 0, len(u.entries))
	for k, e := range u.entries {
		lines = append(lines, fmt.Sprintf("%s{%d,%d};", k, e.id, e.gid))
	}
	sort.Strings(lines)
	return strings.Join(lines, "")
}

// AddUsersAndGroups 是 addUsersAndGroupsImpl 的公开包装，使用默认系统文件路径。
func AddUsersAndGroups() (*UsersAndGroups, error) {
	return addUsersAndGroupsImpl(fileEtcLoginDefs, fileEtcPasswd, fileEtcGroup)
}

// addUsersAndGroupsImpl 将受管用户和组写入 pathUsers 和 pathGroups，
// 根据 pathLoginDef 确定 UID/GID 范围。已有受管用户和组会按范围校验。
// 返回 Users 指针，可用于获取受管用户的 UID 和 GID。
func addUsersAndGroupsImpl(pathLoginDef, pathUsers, pathGroups string) (*UsersAndGroups, error) {
	klog.V(1).Info("Adding managed users and groups")
	klog.V(klogLevel).Infof("Parsing %q", pathLoginDef)

	// 读取并解析 /etc/login.def。某些发行版缺少此文件，属于非标准情况；
	// 出错时向 parseLoginDefs() 传空字符串，回退到默认值。
	var loginDef string
	f, close, err := openFileWithLock(pathLoginDef)
	if err != nil {
		klog.V(1).Infof("Could not open %q, using default system limits: %v", pathLoginDef, err)
	} else {
		loginDef, err = readFile(f)
		if err != nil {
			klog.V(1).Infof("Could not read %q, using default system limits: %v", pathLoginDef, err)
		}
		close()
	}
	limits, err := parseLoginDefs(loginDef)
	if err != nil {
		return nil, err
	}

	klog.V(klogLevel).Infof("Using system UID/GID limits: %+v", limits)
	klog.V(klogLevel).Infof("Parsing %q and %q", pathUsers, pathGroups)

	// 加锁打开 /etc/passwd 和 /etc/group。
	fUsers, close, err := openFileWithLock(pathUsers)
	if err != nil {
		return nil, err
	}
	defer close()
	fGroups, close, err := openFileWithLock(pathGroups)
	if err != nil {
		return nil, err
	}
	defer close()

	// 读取文件。
	fileUsers, err := readFile(fUsers)
	if err != nil {
		return nil, err
	}
	fileGroups, err := readFile(fGroups)
	if err != nil {
		return nil, err
	}

	// 解析文件。
	users, err := parseEntries(fileUsers, totalFieldsUser)
	if err != nil {
		return nil, errors.Wrapf(err, "could not parse %q", pathUsers)
	}
	groups, err := parseEntries(fileGroups, totalFieldsGroup)
	if err != nil {
		return nil, errors.Wrapf(err, "could not parse %q", pathGroups)
	}

	klog.V(klogLevel).Info("Validating existing users and groups")

	// 根据范围校验已有受管条目。
	usersToCreate, groupsToCreate, err := validateEntries(users, groups, limits)
	if err != nil {
		return nil, errors.Wrap(err, "error validating existing users and groups")
	}

	// 分配 UID/GID 给用户和组。
	allocUIDs, err := allocateIDs(users, limits.minUID, limits.maxUID, len(usersToCreate))
	if err != nil {
		return nil, err
	}
	allocGIDs, err := allocateIDs(groups, limits.minGID, limits.maxGID, len(groupsToCreate))
	if err != nil {
		return nil, err
	}
	if err := assignUserAndGroupIDs(groups, usersToCreate, groupsToCreate, allocUIDs, allocGIDs); err != nil {
		return nil, err
	}

	if len(usersToCreate) > 0 {
		klog.V(klogLevel).Infof("Adding users: %s", entriesToString(usersToCreate))
	}
	if len(groupsToCreate) > 0 {
		klog.V(klogLevel).Infof("Adding groups: %s", entriesToString(groupsToCreate))
	}

	// 添加用户和组。
	fileUsers = addEntries(fileUsers, usersToCreate, createUser)
	fileGroups = addEntries(fileGroups, groupsToCreate, createGroup)

	// 写入文件。
	klog.V(klogLevel).Infof("Writing %q and %q", pathUsers, pathGroups)
	if err := writeFile(fUsers, fileUsers); err != nil {
		return nil, err
	}
	if err := writeFile(fGroups, fileGroups); err != nil {
		return nil, err
	}

	// 准备用户和组映射。
	var usersConcat []*entry
	usersConcat = append(usersConcat, users...)
	usersConcat = append(usersConcat, usersToCreate...)
	mapUsers, err := entriesToEntryMap(usersConcat, usersToCreateSpec)
	if err != nil {
		return nil, err
	}

	var groupsConcat []*entry
	groupsConcat = append(groupsConcat, groups...)
	groupsConcat = append(groupsConcat, groupsToCreate...)
	mapGroups, err := entriesToEntryMap(groupsConcat, groupsToCreateSpec)
	if err != nil {
		return nil, err
	}
	return &UsersAndGroups{Users: mapUsers, Groups: mapGroups}, nil
}

// RemoveUsersAndGroups 是 removeUsersAndGroupsImpl 的公开包装，使用默认系统文件路径。
func RemoveUsersAndGroups() error {
	return removeUsersAndGroupsImpl(fileEtcPasswd, fileEtcGroup)
}

// removeUsersAndGroupsImpl 从 pathUsers 和 pathGroups 指定的文件中移除受管用户和组。
func removeUsersAndGroupsImpl(pathUsers, pathGroups string) error {
	klog.V(1).Info("Removing managed users and groups")
	klog.V(klogLevel).Infof("Opening %q and %q", pathUsers, pathGroups)

	// 打开 /etc/passwd 和 /etc/group。
	fUsers, close, err := openFileWithLock(pathUsers)
	if err != nil {
		return err
	}
	defer close()
	fGroups, close, err := openFileWithLock(pathGroups)
	if err != nil {
		return err
	}
	defer close()

	// 读取文件。
	fileUsers, err := readFile(fUsers)
	if err != nil {
		return err
	}
	fileGroups, err := readFile(fGroups)
	if err != nil {
		return err
	}

	klog.V(klogLevel).Infof("Removing users: %s", entriesToString(usersToCreateSpec))
	klog.V(klogLevel).Infof("Removing groups: %s", entriesToString(groupsToCreateSpec))

	// 删除用户和组。
	fileUsers, _ = removeEntries(fileUsers, usersToCreateSpec)
	fileGroups, _ = removeEntries(fileGroups, groupsToCreateSpec)

	klog.V(klogLevel).Infof("Writing %q and %q", pathUsers, pathGroups)

	// 写入文件。
	if err := writeFile(fUsers, fileUsers); err != nil {
		return err
	}
	if err := writeFile(fGroups, fileGroups); err != nil {
		return err
	}

	return nil
}

// parseLoginDefs 解析 /etc/login.defs 中的 UID/GID 系统范围；传空字符串返回默认值，
// UID 和 GID 默认均为 100-999。
func parseLoginDefs(file string) (*limits, error) {
	l := *defaultLimits
	if len(file) == 0 {
		return &l, nil
	}
	var mapping = map[string]*int64{
		"SYS_UID_MIN": &l.minUID,
		"SYS_UID_MAX": &l.maxUID,
		"SYS_GID_MIN": &l.minGID,
		"SYS_GID_MAX": &l.maxGID,
	}
	lines := strings.Split(file, "\n")
	for i, line := range lines {
		for k, v := range mapping {
			// 每行必须以某个定义项开头。
			if !strings.HasPrefix(line, k) {
				continue
			}
			line = strings.TrimPrefix(line, k)
			line = strings.TrimSpace(line)
			val, err := strconv.ParseInt(line, 10, 64)
			if err != nil {
				return nil, errors.Wrapf(err, "could not parse value for %s at line %d", k, i)
			}
			*v = val
		}
	}
	return &l, nil
}

// parseEntries 可解析格式相似的 /etc/passwd 或 /etc/group，返回文件中的条目切片。
// https://www.cyberciti.biz/faq/understanding-etcpasswd-file-format/
// https://www.cyberciti.biz/faq/understanding-etcgroup-file/
func parseEntries(file string, totalFields int) ([]*entry, error) {
	if totalFields != totalFieldsUser && totalFields != totalFieldsGroup {
		return nil, errors.Errorf("unsupported total fields for entry parsing: %d", totalFields)
	}
	lines := strings.Split(file, "\n")
	entries := []*entry{}
	for i, line := range lines {
		line = strings.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		fields := strings.Split(line, ":")
		if len(fields) != totalFields {
			return nil, errors.Errorf("entry must have %d fields separated by ':', "+
				"got %d at line %d: %s", totalFields, len(fields), i, line)
		}
		id, err := strconv.ParseInt(fields[2], 10, 64)
		if err != nil {
			return nil, errors.Wrapf(err, "error parsing id at line %d", i)
		}
		entry := &entry{name: fields[0], id: id}
		if totalFields == totalFieldsGroup {
			entry.userNames = strings.Split(fields[3], ",")
		} else {
			gid, err := strconv.ParseInt(fields[3], 10, 64)
			if err != nil {
				return nil, errors.Wrapf(err, "error parsing GID at line %d", i)
			}
			entry.gid = gid
			entry.shell = fields[6]
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// validateEntries 根据范围、用户与组映射及规格校验用户和组条目，
// 返回待创建的用户和组切片；已有用户或组不符合要求时返回错误。
func validateEntries(users, groups []*entry, limits *limits) ([]*entry, []*entry, error) {
	u := []*entry{}
	g := []*entry{}
	// 校验用户。
	for _, uc := range usersToCreateSpec {
		for _, user := range users {
			if uc.name != user.name {
				continue
			}
			// 找到已有用户。
			if user.id < limits.minUID || user.id > limits.maxUID {
				return nil, nil, errors.Errorf("UID %d for user %q is outside the system UID range: %d - %d",
					user.id, user.name, limits.minUID, limits.maxUID)
			}
			if user.shell != noshell {
				return nil, nil, errors.Errorf("user %q has unexpected shell %q; expected %q",
					user.name, user.shell, noshell)
			}
			for _, g := range groups {
				if g.id != user.gid {
					continue
				}
				// 找到与用户 GID 匹配的组 GID。
				if g.name != uc.name {
					return nil, nil, errors.Errorf("user %q has GID %d but the group with that GID is not named %q",
						uc.name, g.id, uc.name)
				}
				goto skipUser // goto skipUser // 组 GID 和名称有效，跳过
			}
			return nil, nil, errors.Errorf("could not find group with GID %d for user %q", user.gid, user.name)
		}
		u = append(u, uc)
	skipUser:
	}
	// 校验组。
	for _, gc := range groupsToCreateSpec {
		for _, group := range groups {
			if gc.name != group.name {
				continue
			}
			if group.id < limits.minGID || group.id > limits.maxGID {
				return nil, nil, errors.Errorf("GID %d for user %q is outside the system UID range: %d - %d",
					group.id, group.name, limits.minGID, limits.maxGID)
			}
			u1 := strings.Join(gc.userNames, ",")
			u2 := strings.Join(group.userNames, ",")
			if u1 != u2 {
				return nil, nil, errors.Errorf("expected users %q for group %q; got %q",
					u1, gc.name, u2)
			}
			goto skipGroup // goto skipGroup // 组包含有效用户，跳过
		}
		g = append(g, gc)
	skipGroup:
	}
	return u, g, nil
}

// allocateIDs 根据 ID 最小和最大值，为条目列表分配指定数量的 ID。
func allocateIDs(entries []*entry, min, max int64, total int) ([]int64, error) {
	if total == 0 {
		return []int64{}, nil
	}
	ids := make([]int64, 0, total)
	for i := min; i < max+1; i++ {
		i64 := int64(i)
		for _, e := range entries {
			if i64 == e.id {
				goto continueLoop
			}
		}
		ids = append(ids, i64)
		if len(ids) == total {
			return ids, nil
		}
	continueLoop:
	}
	return nil, errors.Errorf("could not allocate %d IDs based on existing entries in the range: %d - %d",
		total, min, max)
}

// addEntries 根据 createEntry 函数将条目追加到 /etc/passwd 或 /etc/group 内容，返回更新后的内容。
func addEntries(file string, entries []*entry, createEntry func(*entry) string) string {
	out := file
	newLines := make([]string, 0, len(entries))
	for _, e := range entries {
		newLines = append(newLines, createEntry(e))
	}
	newLinesStr := ""
	if len(newLines) > 0 {
		if !strings.HasSuffix(out, "\n") { // if !strings.HasSuffix(out, "
			// ") { // 缺少换行时追加
			newLinesStr = "\n"
		}
		newLinesStr += strings.Join(newLines, "\n") + "\n"
	}
	return out + newLinesStr
}

// removeEntries 按名称从 /etc/passwd 或 /etc/group 内容中删除条目，
// 返回更新后的内容和删除的条目数。
func removeEntries(file string, entries []*entry) (string, int) {
	lines := strings.Split(file, "\n")
	total := len(lines) - len(entries)
	if total < 0 {
		total = 0
	}
	newLines := make([]string, 0, total)
	removed := 0
	for _, line := range lines {
		for _, entry := range entries {
			if strings.HasPrefix(line, entry.name+":") {
				removed++
				goto continueLoop
			}
		}
		newLines = append(newLines, line)
	continueLoop:
	}
	return strings.Join(newLines, "\n"), removed
}

// assignUserAndGroupIDs 根据已有组、待创建用户和组以及给定 UID/GID 列表分配 ID。
// UID 或 GID 不足时返回错误，不执行其他校验。
func assignUserAndGroupIDs(groups, usersToCreate, groupsToCreate []*entry, uids, gids []int64) error {
	if len(gids) < len(groupsToCreate) {
		return errors.Errorf("not enough GIDs to assign to groups: have %d, want %d", len(gids), len(groupsToCreate))
	}
	if len(uids) < len(usersToCreate) {
		return errors.Errorf("not enough UIDs to assign to users: have %d, want %d", len(uids), len(usersToCreate))
	}
	for i := range groupsToCreate {
		groupsToCreate[i].id = gids[i]
	}
	// 合并新旧组列表以查找匹配的 GID。
	groupsConcat := append([]*entry{}, groups...)
	groupsConcat = append(groupsConcat, groupsToCreate...)
	for i := range usersToCreate {
		usersToCreate[i].id = uids[i]
		for _, g := range groupsConcat {
			if usersToCreate[i].name == g.name {
				usersToCreate[i].gid = g.id
				break
			}
		}
	}
	return nil
}

// createGroup 根据 entry 辅助创建组。
func createGroup(e *entry) string {
	return fmt.Sprintf("%s:x:%d:%s", e.name, e.id, strings.Join(e.userNames, ","))
}

// createUser 根据 entry 辅助创建用户。
func createUser(e *entry) string {
	return fmt.Sprintf("%s:x:%d:%d:::/bin/false", e.name, e.id, e.gid)
}

// entriesToEntryMap 将条目列表转换为 EntryMap。
func entriesToEntryMap(entries, spec []*entry) (*EntryMap, error) {
	m := map[string]*entry{}
	for _, spec := range spec {
		for _, e := range entries {
			if spec.name == e.name {
				entry := *e
				m[e.name] = &entry
				goto continueLoop
			}
		}
		return nil, errors.Errorf("could not find entry %q in the list", spec.name)
	continueLoop:
	}
	return &EntryMap{entries: m}, nil
}

// entriesToString 将条目列表转换为字符串。
func entriesToString(entries []*entry) string {
	lines := make([]string, 0, len(entries))
	for _, e := range entries {
		lines = append(lines, e.name)
	}
	sort.Strings(lines)
	return strings.Join(lines, ",")
}

// openFileWithLock 以独占写锁打开 path 指定文件。返回的 close() 应被调用以释放锁并关闭文件；
// 无法及时获取锁时函数失败。
func openFileWithLock(path string) (f *os.File, close func(), err error) {
	f, err = os.OpenFile(path, os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, nil, err
	}
	deadline := time.Now().Add(time.Second * 5)
	for {
		// 其他进程持有写锁时，此调用将返回错误。使用 F_SETLK 而非 F_SETLKW，
		// 避免失控进程获取独占锁后无限阻塞当前调用。
		// https://man7.org/linux/man-pages/man2/fcntl.2.html
		lock := syscall.Flock_t{Type: syscall.F_WRLCK}
		if err = syscall.FcntlFlock(f.Fd(), syscall.F_SETLK, &lock); err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
		if time.Now().After(deadline) {
			err = errors.Wrapf(err, "timeout attempting to obtain lock on file %q", path)
			break
		}
	}
	if err != nil {
		_ = f.Close()
		return nil, nil, err
	}
	close = func() {
		// 文件操作结束后应调用此函数，以解锁并关闭文件。
		unlock := syscall.Flock_t{Type: syscall.F_UNLCK}
		_ = syscall.FcntlFlock(f.Fd(), syscall.F_SETLK, &unlock)
		_ = f.Close()
	}
	return f, close, nil
}

// readFile 将 File 读取为字符串。
func readFile(f *os.File) (string, error) {
	buf := bytes.NewBuffer(nil)
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return "", err
	}
	if _, err := io.Copy(buf, f); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// writeFile 将字符串写入 File。
func writeFile(f *os.File, str string) error {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if _, err := f.Write([]byte(str)); err != nil {
		return err
	}
	if err := f.Truncate(int64(len(str))); err != nil {
		return err
	}
	return nil
}

// UpdatePathOwnerAndPermissions 更新给定路径的所有者和权限；目录不会递归更新。
func UpdatePathOwnerAndPermissions(path string, uid, gid int64, perms uint32) error {
	if err := os.Chown(path, int(uid), int(gid)); err != nil {
		return errors.Wrapf(err, "failed to update owner of %q to uid: %d and gid: %d", path, uid, gid)
	}
	fm := os.FileMode(perms)
	if err := os.Chmod(path, fm); err != nil {
		return errors.Wrapf(err, "failed to update permissions of %q to %s", path, fm.String())
	}
	return nil
}

// UpdatePathOwner 递归更新目录所有者，等价于执行 `chown -R uid:gid /path/to/dir`。
func UpdatePathOwner(dirPath string, uid, gid int64) error {
	err := filepath.WalkDir(dirPath, func(path string, d os.DirEntry, err error) error {
		if err := os.Chown(path, int(uid), int(gid)); err != nil {
			return errors.Wrapf(err, "failed to update owner of %q to uid: %d and gid: %d", path, uid, gid)
		}
		return nil
	})
	return err
}
