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

package pfs

import (
	"errors"
	"fmt"
	"github.com/AliyunContainerService/flexvolume/provider/utils"
	log "github.com/sirupsen/logrus"
	"os"
	"path"
	"path/filepath"
	"strings"
)

type PfsOptions struct {
	Server     string `json:"server"`
	Path       string `json:"path"`
	SubPath    string `json:"subPath"`
	SkId       string `json:"skId"`
	VolumeName string `json:"kubernetes.io/pvOrVolumeName"`
}

const (
	PFS_TEMP_MNTPath = "/mnt/eci/pfs/"
)

type PfsPlugin struct {
}

func (p *PfsPlugin) NewOptions() interface{} {
	return &PfsOptions{}
}

// Init support volume metric
func (p *PfsPlugin) Init() utils.Result {
	return utils.Succeed()
}

// Mount pfs support mount and umount
func (p *PfsPlugin) Mount(opts interface{}, mountPath string) utils.Result {
	log.Infof("Pfs Volume Mount: %s", strings.Join(os.Args, ","))

	opt := opts.(*PfsOptions)
	if err := p.checkOptions(opt); err != nil {
		log.Errorf("Pfs, Options is illegal: %s", err.Error())
		utils.FinishError("Pfs, Options is illegal: " + err.Error())
	}

	if utils.IsMounted(mountPath) {
		log.Infof("Pfs, Mount Path Already Mounted, path: %s", mountPath)
		return utils.Result{Status: "Success"}
	}

	// Create Mount Path
	if err := utils.CreateDest(mountPath); err != nil {
		log.Errorf("Pfs, Mount error with create Path fail: %s", mountPath)
		utils.FinishError("Pfs, Mount error with create Path fail: " + mountPath)
	}

	// Set config (/etc/pfs/pfs-client.conf)
	doPfsConfig(opt)

	// Create sub path
	if opt.SubPath != "" && opt.SubPath != "/" {
		p.createPfsSubDir(opt)
	}

	// Do mount
	if _, err := doMount(mountPath, opt); err != nil {
		log.Errorf("doMount, start pfs-client with error: %s", err.Error())
		utils.FinishError("Pfs, Mount Pfs fail: " + err.Error())
	}
	log.Infof("Pfs Volume Mount to %s Success.", mountPath)
	return utils.Result{Status: "Success"}
}

func doPfsConfig(opt *PfsOptions) {
	serverCmd := fmt.Sprintf("sed -i '/^cluster_addr =$/s/$/ %s/' /etc/pfs/pfs-client.conf", opt.Server)
	if _, err := utils.Run(serverCmd); err != nil {
		log.Errorf("doPfsConfig, set server fail with error: %s", err.Error())
	}
	udpPortCmd := fmt.Sprintf("sed -i '2 {/conn_mgr_port_udp/! s/^/conn_mgr_port_udp = 8000\\n/}' /etc/pfs/pfs-client.conf")
	if _, err := utils.Run(udpPortCmd); err != nil {
		log.Errorf("doPfsConfig, set udp port fail with error: %s", err.Error())
	}
	nicCmd := fmt.Sprintf("sed -i '/^conn_nics =$/s/$/ eth0/' /etc/pfs/pfs-client.conf")
	if _, err := utils.Run(nicCmd); err != nil {
		log.Errorf("doPfsConfig, set nics fail with error: %s", err.Error())
	}
}

func doMount(mountPath string, opt *PfsOptions) (string, error) {
	// 1.set /etc/pfs/pfs-mounts.conf
	pfsPath := filepath.Join(opt.Path, opt.SubPath)
	nicCmd := fmt.Sprintf("echo '%s /etc/pfs/pfs-client.conf %s %s' > /etc/pfs/pfs-mounts.conf", mountPath, pfsPath, opt.SkId)
	if _, err := utils.Run(nicCmd); err != nil {
		log.Errorf("doPfsConfig, set nics fail with error: %s", err.Error())
	}
	// 2.start pfs client
	return utils.Run("systemctl start pfs-client")
}

// 1. mount to /mnt/eci/pfs/temp first
// 2. run mkdir for sub directory
// 3. umount the temp directory
func (p *PfsPlugin) createPfsSubDir(opt *PfsOptions) {
	// step 1: create mount path
	rootTempPath := filepath.Join(PFS_TEMP_MNTPath)
	if err := utils.CreateDest(rootTempPath); err != nil {
		utils.FinishError("Create Pfs temp Directory err: " + err.Error())
	}
	if utils.IsMounted(rootTempPath) {
		utils.Umount(rootTempPath)
	}

	subOpt := &PfsOptions{
		Server:  opt.Server,
		Path:    opt.Path,
		SubPath: "/",
		SkId:    opt.SkId,
	}

	// step 2: do mount
	if _, err := doMount(rootTempPath, subOpt); err != nil {
		utils.FinishError("CreatePfsSubDir, Mount to temp directory fail: " + err.Error())
	}

	// step 3: create sub directory
	subPath := path.Join(rootTempPath, opt.SubPath)
	if err := utils.CreateDest(subPath); err != nil {
		utils.FinishError("CreatePfsSubDir, Create Sub Directory err: " + err.Error())
	}

	// step 4: umount after create, stop pfs client
	if _, err := utils.Run("systemctl stop pfs-client"); err != nil {
		utils.FinishError("CreatePfsSubDir, umount fail: " + err.Error())
	}
	log.Infof("Create Sub Directory success for sub path: %s", opt.SubPath)
}

func (p *PfsPlugin) Unmount(mountPoint string) utils.Result {
	log.Infof("Pfs Volume Umount: %s", strings.Join(os.Args, ","))

	if !utils.IsMounted(mountPoint) {
		log.Infof("Path not mounted, skipped: %s", mountPoint)
		return utils.Succeed()
	}

	if _, err := utils.Run("systemctl stop pfs-client"); err != nil {
		utils.FinishError("Pfs, Umount Pfs Fail: " + err.Error())
	}

	log.Infof("Umount Pfs Successful: %s", mountPoint)
	return utils.Succeed()
}

func (p *PfsPlugin) Attach(opts interface{}, nodeName string) utils.Result {
	return utils.NotSupport()
}

func (p *PfsPlugin) Detach(device string, nodeName string) utils.Result {
	return utils.NotSupport()
}

// Support
func (p *PfsPlugin) Getvolumename(opts interface{}) utils.Result {
	opt := opts.(*PfsOptions)
	return utils.Result{
		Status:     "Success",
		VolumeName: opt.VolumeName,
	}
}

// Not Support
func (p *PfsPlugin) Waitforattach(devicePath string, opts interface{}) utils.Result {
	return utils.NotSupport()
}

// Not Support
func (p *PfsPlugin) Mountdevice(mountPath string, opts interface{}) utils.Result {
	return utils.NotSupport()
}

// Pfs options
func (p *PfsPlugin) checkOptions(opt *PfsOptions) error {
	// Pfs Server url
	if opt.Server == "" {
		return errors.New("pfs: server is empty")
	}

	// Pfs path
	if opt.Path == "" {
		return errors.New("pfs: path is empty")
	}

	// Pfs skId
	if opt.SkId == "" {
		return errors.New("pfs: SkId is empty")
	}

	opt.SubPath = strings.TrimSpace(opt.SubPath)
	if opt.SubPath != "" && !strings.HasPrefix(opt.SubPath, "/") {
		opt.SubPath = "/" + opt.SubPath
	}
	if opt.SubPath == "" {
		opt.SubPath = "/"
	}
	return nil
}

// Not Support
func (p *PfsPlugin) ExpandVolume(opt interface{}, devicePath, newSize, oldSize string) utils.Result {
	return utils.NotSupport()
}

// Not Support
func (p *PfsPlugin) ExpandFS(opt interface{}, devicePath, deviceMountPath, newSize, oldSize string) utils.Result {
	return utils.NotSupport()
}
