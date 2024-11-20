package disk

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/AliyunContainerService/flexvolume/provider/utils"
	log "github.com/sirupsen/logrus"
)

// DiskOptions define the disk parameters
type DiskOptions struct {
	VolumeName string `json:"kubernetes.io/pvOrVolumeName"`
	FsType     string `json:"kubernetes.io/fsType"`
	VolumeId   string `json:"volumeId"`
}

// DiskPlugin define DiskPlugin
type DiskPlugin struct {
}

// NewOptions define NewOptions
func (p *DiskPlugin) NewOptions() interface{} {
	return &DiskOptions{}
}

// Init define Init for DiskPlugin
func (p *DiskPlugin) Init() utils.Result {
	return utils.Succeed()
}

// Attach attach with NodeName and Options
// Attach: nodeName: regionId.instanceId, exammple: cn-hangzhou.i-bp12gei4ljuzilgwzahc
// Attach: options: {"kubernetes.io/fsType": "", "kubernetes.io/pvOrVolumeName": "", "kubernetes.io/readwrite": "", "volumeId":""}
func (p *DiskPlugin) Attach(opts interface{}, nodeName string) utils.Result {
	log.Infof("Disk Plugin Attach: %s", strings.Join(os.Args, ","))
	opt := opts.(*DiskOptions)
	cmd := fmt.Sprintf("mount | grep ecloud~disk/%s", opt.VolumeName)
	if out, err := utils.Run(cmd); err == nil {
		devicePath := strings.Split(strings.TrimSpace(out), " ")[0]
		log.Infof("Disk Already Attached, DiskId: %s, Device: %s", opt.VolumeName, devicePath)
		return utils.Result{Status: "Success", Device: devicePath}
	}
	devicePath, err := GetDevicePathByScsiCmd(opt.VolumeId)
	if err != nil {
		log.Errorf("Failed to get device path from scsi cmd of volume %s, with error: %v", opt.VolumeId, err)
		return utils.Fail("Disk, Can not get disk: "+opt.VolumeId+", with error: %v", err)
	} else if devicePath == "" {
		log.Warningf("With scsi cmd,  Device path was empty for volumeID: %s", opt.VolumeId)
		return utils.Fail("With scsi cmd,  Device path was empty for volumeID: " + opt.VolumeId)
	} else {
		return utils.Result{Status: "Success", Device: devicePath}
	}
}

// Detach current kubelet call detach not provide plugin spec;
// this issue is tracked by: https://github.com/kubernetes/kubernetes/issues/52590
func (p *DiskPlugin) Detach(volumeName string, nodeName string) utils.Result {
	log.Infof("Disk Plugin Detach: %s", strings.Join(os.Args, ","))
	return utils.Succeed()
}

// Mount Not Support
func (p *DiskPlugin) Mount(opts interface{}, mountPath string) utils.Result {
	log.Infof("Disk Plugin Mount: %s", strings.Join(os.Args, ","))
	return utils.NotSupport()
}

// Unmount Support, to fix umount bug;
func (p *DiskPlugin) Unmount(mountPoint string) utils.Result {
	log.Infof("Disk, Starting to Unmount: %s", mountPoint)
	p.doUnmount(mountPoint)
	log.Infof("Disk, Unmount Successful: %s", mountPoint)
	return utils.Succeed()
}

func (p *DiskPlugin) doUnmount(mountPoint string) {
	if err := UnmountMountPoint(mountPoint); err != nil {
		utils.FinishError("Disk, Failed to Unmount: " + mountPoint + err.Error())
	}

	// issue: below directory can not be umounted
	// /var/lib/kubelet/plugins/kubernetes.io/flexvolume/ecloud/disk/mounts/d-2zefwuq9sv0gkxqrll5t
	diskMntPath := "/var/lib/kubelet/plugins/kubernetes.io/flexvolume/ecloud/disk/mounts/" + filepath.Base(mountPoint)
	if err := UnmountMountPoint(diskMntPath); err != nil {
		utils.FinishError("Disk, Failed to Unmount: " + diskMntPath + " with error: " + err.Error())
	}
}

// UnmountMountPoint Unmount host mount path
func UnmountMountPoint(mountPath string) error {
	// check mountpath is exist
	if pathExists, pathErr := utils.PathExists(mountPath); pathErr != nil {
		return pathErr
	} else if !pathExists {
		return nil
	}

	// check mountPath is mountPoint
	var notMnt bool
	var err error
	notMnt, err = utils.IsLikelyNotMountPoint(mountPath)
	if err != nil {
		return err
	}
	if notMnt {
		log.Warningf("Warning: %q is not a mountpoint, deleting", mountPath)
		return os.Remove(mountPath)
	}

	// Unmount the mount path
	mntCmd := fmt.Sprintf("umount -f %s", mountPath)
	if _, err := utils.Run(mntCmd); err != nil {
		return err
	}
	notMnt, mntErr := utils.IsLikelyNotMountPoint(mountPath)
	if mntErr != nil {
		return err
	}
	if notMnt {
		if err := os.Remove(mountPath); err != nil {
			log.Warningf("Warning: deleting mountPath %s, with error: %s", mountPath, err.Error())
			return err
		}
		return nil
	}
	return fmt.Errorf("Failed to unmount path")
}

// Getvolumename Support
func (p *DiskPlugin) Getvolumename(opts interface{}) utils.Result {
	opt := opts.(*DiskOptions)
	return utils.Result{
		Status:     "Success",
		VolumeName: opt.VolumeName,
	}
}

// Waitforattach Not Support
func (p *DiskPlugin) Waitforattach(devicePath string, opts interface{}) utils.Result {
	opt := opts.(*DiskOptions)
	if devicePath == "" {
		utils.FinishError("Waitforattach, devicePath is empty, cannot used for Volume: " + opt.VolumeName)
	}
	if !utils.IsFileExisting(devicePath) {
		utils.FinishError("Waitforattach, devicePath: " + devicePath + " is not exist, cannot used for Volume: " + opt.VolumeName)
	}

	log.Infof("Waitforattach, wait for attach: %s, %s", devicePath, opt.VolumeName)
	return utils.Result{
		Status: "Success",
		Device: devicePath,
	}
}

// Mountdevice Not Support
func (p *DiskPlugin) Mountdevice(mountPath string, opts interface{}) utils.Result {
	return utils.NotSupport()
}

func GetDevicePathByScsiCmd(volumeId string) (string, error) {
	deviceList, err := getDiskListWithLsBlk()
	if err != nil {
		log.Errorf("Command lsblk execution error  %v", err)
		return "", err
	}
	device := strings.TrimSuffix(string(deviceList), "\n")
	deviceSlice := strings.Split(device, "\n")
	for _, device := range deviceSlice {
		devicePath := fmt.Sprintf("/dev/%s", device)
		cmdInfo, err := getDiskIdFromUdev(devicePath)
		if err != nil {
			log.Errorf("Command scsi_id execution error  %v", err)
			return "", err
		}
		stringId := strings.TrimSuffix(string(cmdInfo), "\n")
		if stringId == volumeId {
			log.Infof("Succeed to find volumeID: %q devicePath: %q", volumeId, devicePath)
			return devicePath, nil
		}
	}
	log.Warningf("Failed to find device for the volumeID: %q by serial ID", volumeId)
	return "", nil
}

func getDiskIdFromUdev(devicePath string) ([]byte, error) {
	cmdStr := fmt.Sprintf("/lib/udev/scsi_id -g -x %s |grep ID_SCSI_SERIAL|cut -d \"=\"  -f 2", devicePath)
	cmd := exec.Command("sh", "-c", cmdStr)
	return cmd.CombinedOutput()
}
func getDiskListWithLsBlk() ([]byte, error) {
	cmdStr := fmt.Sprintf("lsblk -l | grep disk | awk '{print $1}'")
	cmd := exec.Command("sh", "-c", cmdStr)
	return cmd.CombinedOutput()
}
