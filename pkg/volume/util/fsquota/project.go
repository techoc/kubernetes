//go:build linux

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

package fsquota

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"

	"golang.org/x/sys/unix"
	"k8s.io/kubernetes/pkg/volume/util/fsquota/common"
)

var projectsFile = "/etc/projects"
var projidFile = "/etc/projid"

var projectsParseRegexp = regexp.MustCompilePOSIX("^([[:digit:]]+):(.*)$")
var projidParseRegexp = regexp.MustCompilePOSIX("^([^#][^:]*):([[:digit:]]+)$")

// quotaIDLock 保证多 goroutine 同时分配配额 ID 时不会冲突。
var quotaIDLock sync.RWMutex

// 限制查找次数，避免找不到空闲配额时陷入死循环。
const maxUnusedQuotasToSearch = 128

// projectType 表示 /etc/projects 或 /etc/projid 中的一行。
type projectType struct {
	isValid bool // 为 false 表示该行需要删除
	id      common.QuotaID
	data    string // projid 文件中是项目名，projects 文件中是目录
	line    string
}

// projectsList 保存两个项目文件的解析结果。
type projectsList struct {
	projects []projectType
	projid   []projectType
}

// projFilesAreOK 确认项目文件不存在或是普通文件，防止经由符号链接篡改。
func projFilesAreOK() error {
	if sf, err := os.Lstat(projectsFile); err != nil || sf.Mode().IsRegular() {
		if sf, err := os.Lstat(projidFile); err != nil || sf.Mode().IsRegular() {
			return nil
		}
		return fmt.Errorf("%s exists but is not a plain file, cannot continue", projidFile)
	}
	return fmt.Errorf("%s exists but is not a plain file, cannot continue", projectsFile)
}

// lockFile 对文件加排他锁。
func lockFile(file *os.File) error {
	return unix.Flock(int(file.Fd()), unix.LOCK_EX)
}

// unlockFile 释放文件锁。
func unlockFile(file *os.File) error {
	return unix.Flock(int(file.Fd()), unix.LOCK_UN)
}

// openAndLockProjectFiles 打开并锁定 /etc/projects 和 /etc/projid，不存在则创建。
func openAndLockProjectFiles() (*os.File, *os.File, error) {
	// 先确认项目文件都不是符号链接。
	if err := projFilesAreOK(); err != nil {
		return nil, nil, fmt.Errorf("system project files failed verification: %v", err)
	}
	// 不直接改写原文件：写入临时文件后再重命名覆盖，避免读到半写状态。
	fProjects, err := os.OpenFile(projectsFile, os.O_RDONLY|os.O_CREATE, 0644)
	if err != nil {
		err = fmt.Errorf("unable to open %s: %v", projectsFile, err)
		return nil, nil, err
	}
	fProjid, err := os.OpenFile(projidFile, os.O_RDONLY|os.O_CREATE, 0644)
	if err == nil {
		// 再次校验，防止校验后被换成了符号链接。
		if err = projFilesAreOK(); err == nil {
			err = lockFile(fProjects)
			if err == nil {
				err = lockFile(fProjid)
				if err == nil {
					return fProjects, fProjid, nil
				}
				// 此处已无其他补救手段。
				err = fmt.Errorf("unable to lock %s: %v", projidFile, err)
				unlockFile(fProjects)
			} else {
				err = fmt.Errorf("unable to lock %s: %v", projectsFile, err)
			}
		} else {
			err = fmt.Errorf("system project files failed re-verification: %v", err)
		}
		fProjid.Close()
	} else {
		err = fmt.Errorf("unable to open %s: %v", projidFile, err)
	}
	fProjects.Close()
	return nil, nil, err
}

// closeProjectFiles 关闭两个文件（同时释放锁），返回其中一个错误。
func closeProjectFiles(fProjects *os.File, fProjid *os.File) error {
	// 关闭失败无法补救，但必须关闭以释放锁。
	var err error
	var err1 error
	if fProjid != nil {
		err = fProjid.Close()
	}
	if fProjects != nil {
		err1 = fProjects.Close()
	}
	if err == nil {
		return err1
	}
	return err
}

// parseProject 解析 /etc/projects 的一行（id:目录）。
func parseProject(l string) projectType {
	if match := projectsParseRegexp.FindStringSubmatch(l); match != nil {
		i, err := strconv.Atoi(match[1])
		if err == nil {
			return projectType{true, common.QuotaID(i), match[2], l}
		}
	}
	return projectType{true, common.BadQuotaID, "", l}
}

// parseProjid 解析 /etc/projid 的一行（名称:id）。
func parseProjid(l string) projectType {
	if match := projidParseRegexp.FindStringSubmatch(l); match != nil {
		i, err := strconv.Atoi(match[2])
		if err == nil {
			return projectType{true, common.QuotaID(i), match[1], l}
		}
	}
	return projectType{true, common.BadQuotaID, "", l}
}

// parseProjFile 用 parser 逐行解析项目文件。
func parseProjFile(f *os.File, parser func(l string) projectType) []projectType {
	var answer []projectType
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		answer = append(answer, parser(scanner.Text()))
	}
	return answer
}

// readProjectFiles 读取并解析两个项目文件。
func readProjectFiles(projects *os.File, projid *os.File) projectsList {
	return projectsList{parseProjFile(projects, parseProject), parseProjFile(projid, parseProjid)}
}

// findAvailableQuota 从 FirstQuota 开始查找空闲配额 ID。
// 查询使用中配额出错时返回错误，最多尝试 maxUnusedQuotasToSearch(128) 次。
func findAvailableQuota(path string, idMap map[common.QuotaID]bool) (common.QuotaID, error) {
	unusedQuotasSearched := 0
	for id := common.FirstQuota; true; id++ {
		if _, ok := idMap[id]; !ok {
			isInUse, err := getApplier(path).QuotaIDIsInUse(id)
			if err != nil {
				return common.BadQuotaID, err
			} else if !isInUse {
				return id, nil
			}
			unusedQuotasSearched++
			if unusedQuotasSearched > maxUnusedQuotasToSearch {
				break
			}
		}
	}
	return common.BadQuotaID, fmt.Errorf("cannot find available quota ID")
}

// addDirToProject 将目录加入项目列表，必要时分配新 ID 和项目名；
// 返回 ID、是否需要写 projid 文件及错误。
func addDirToProject(path string, id common.QuotaID, list *projectsList) (common.QuotaID, bool, error) {
	idMap := make(map[common.QuotaID]bool)
	for _, project := range list.projects {
		if project.data == path {
			if id != common.BadQuotaID && id != project.id {
				return common.BadQuotaID, false, fmt.Errorf("attempt to reassign project ID for %s", path)
			}
			// 目录已在目标项目中，按幂等操作处理。
			return project.id, false, nil
		}
		idMap[project.id] = true
	}
	var needToAddProjid = true
	for _, projid := range list.projid {
		idMap[projid.id] = true
		if projid.id == id && id != common.BadQuotaID {
			needToAddProjid = false
		}
	}
	var err error
	if id == common.BadQuotaID {
		id, err = findAvailableQuota(path, idMap)
		if err != nil {
			return common.BadQuotaID, false, err
		}
		needToAddProjid = true
	}
	if needToAddProjid {
		name := fmt.Sprintf("volume%v", id)
		line := fmt.Sprintf("%s:%v", name, id)
		list.projid = append(list.projid, projectType{true, id, name, line})
	}
	line := fmt.Sprintf("%v:%s", id, path)
	list.projects = append(list.projects, projectType{true, id, path, line})
	return id, needToAddProjid, nil
}

// removeDirFromProject 从项目列表移除目录，最后一个使用者被移除时回收项目 ID；
// 返回是否需要写 projid 文件及错误。
func removeDirFromProject(path string, id common.QuotaID, list *projectsList) (bool, error) {
	if id == common.BadQuotaID {
		return false, fmt.Errorf("attempt to remove invalid quota ID from %s", path)
	}
	foundAt := -1
	countByID := make(map[common.QuotaID]int)
	for i, project := range list.projects {
		if project.data == path {
			if id != project.id {
				return false, fmt.Errorf("attempting to remove quota ID %v from path %s, but expecting ID %v", id, path, project.id)
			} else if foundAt != -1 {
				return false, fmt.Errorf("found multiple quota IDs for path %s", path)
			}
			// 标记为无效比删除切片元素更简单。
			list.projects[i].isValid = false
			foundAt = i
		}
		countByID[project.id]++
	}
	if foundAt == -1 {
		return false, fmt.Errorf("cannot find quota associated with path %s", path)
	}
	if countByID[id] <= 1 {
		// 最后一个条目被移除意味着该配额 ID 不再使用，一并回收。
		for i, projid := range list.projid {
			if projid.id == id {
				list.projid[i].isValid = false
			}
		}
		return true, nil
	}
	return false, nil
}

// writeProjectFile 将有效条目写入临时文件，返回临时文件路径。
func writeProjectFile(base *os.File, projects []projectType) (string, error) {
	oname := base.Name()
	stat, err := base.Stat()
	if err != nil {
		return "", err
	}
	mode := stat.Mode() & os.ModePerm
	f, err := os.CreateTemp(filepath.Dir(oname), filepath.Base(oname))
	if err != nil {
		return "", err
	}
	filename := f.Name()
	if err := os.Chmod(filename, mode); err != nil {
		return "", err
	}
	for _, proj := range projects {
		if proj.isValid {
			if _, err := f.WriteString(fmt.Sprintf("%s\n", proj.line)); err != nil {
				f.Close()
				os.Remove(filename)
				return "", err
			}
		}
	}
	if err := f.Close(); err != nil {
		os.Remove(filename)
		return "", err
	}
	return filename, nil
}

// writeProjectFiles 原子替换项目文件；两个临时文件都写好后再依次重命名。
func writeProjectFiles(fProjects *os.File, fProjid *os.File, writeProjid bool, list projectsList) error {
	tmpProjects, err := writeProjectFile(fProjects, list.projects)
	if err == nil {
		// 两个临时文件都写成功后再重命名，避免只更新其中一个。
		if writeProjid {
			tmpProjid, err := writeProjectFile(fProjid, list.projid)
			if err == nil {
				err = os.Rename(tmpProjid, fProjid.Name())
				if err != nil {
					os.Remove(tmpProjid)
				}
			}
		}
		if err == nil {
			err = os.Rename(tmpProjects, fProjects.Name())
			if err == nil {
				return nil
			}
			// projid 已替换但 projects 替换失败：此时无论怎样回滚都可能
			// 再次失败，只能保留现状。
		}
		os.Remove(tmpProjects)
	}
	return fmt.Errorf("unable to write project files: %v", err)
}

// createProjectID 返回路径对应的项目 ID；若 ID 为 BadQuotaID 且目录尚未加入
// 项目，则分配新 ID。
func createProjectID(path string, ID common.QuotaID) (common.QuotaID, error) {
	quotaIDLock.Lock()
	defer quotaIDLock.Unlock()
	fProjects, fProjid, err := openAndLockProjectFiles()
	if err == nil {
		defer closeProjectFiles(fProjects, fProjid)
		list := readProjectFiles(fProjects, fProjid)
		var writeProjid bool
		ID, writeProjid, err = addDirToProject(path, ID, &list)
		if err == nil && ID != common.BadQuotaID {
			if err = writeProjectFiles(fProjects, fProjid, writeProjid, list); err == nil {
				return ID, nil
			}
		}
	}
	return common.BadQuotaID, fmt.Errorf("createProjectID %s %v failed %v", path, ID, err)
}

// removeProjectID 从项目文件中移除路径对应的配额 ID。
func removeProjectID(path string, ID common.QuotaID) error {
	if ID == common.BadQuotaID {
		return fmt.Errorf("attempting to remove invalid quota ID %v", ID)
	}
	quotaIDLock.Lock()
	defer quotaIDLock.Unlock()
	fProjects, fProjid, err := openAndLockProjectFiles()
	if err == nil {
		defer closeProjectFiles(fProjects, fProjid)
		list := readProjectFiles(fProjects, fProjid)
		var writeProjid bool
		writeProjid, err = removeDirFromProject(path, ID, &list)
		if err == nil {
			if err = writeProjectFiles(fProjects, fProjid, writeProjid, list); err == nil {
				return nil
			}
		}
	}
	return fmt.Errorf("removeProjectID %s %v failed %v", path, ID, err)
}
