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

package disk

import (
	"errors"
	"fmt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/aliyun/alibaba-cloud-sdk-go/services/ecs"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-csi/drivers/pkg/csi-common"
	"github.com/kubernetes-sigs/alibaba-cloud-csi-driver/pkg/log"
	"golang.org/x/net/context"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/kubernetes/pkg/util/resizefs"
	utilexec "k8s.io/utils/exec"
	k8smount "k8s.io/utils/mount"

	"github.com/kubernetes-sigs/alibaba-cloud-csi-driver/pkg/utils"
)

type nodeServer struct {
	zone              string
	maxVolumesPerNode int64
	nodeID            string
	mounter           utils.Mounter
	k8smounter        k8smount.Interface
	clientSet         *kubernetes.Clientset
	*csicommon.DefaultNodeServer
}

const (
	// DiskStatusInuse disk inuse status
	DiskStatusInuse = "In_use"
	// DiskStatusAttaching disk attaching status
	DiskStatusAttaching = "Attaching"
	// DiskStatusAvailable disk available status
	DiskStatusAvailable = "Available"
	// DiskStatusAttached disk attached status
	DiskStatusAttached = "attached"
	// DiskStatusDetached disk detached status
	DiskStatusDetached = "detached"
	// SharedEnable tag
	SharedEnable = "shared"
	// SysConfigTag tag
	SysConfigTag = "sysConfig"
	// MkfsOptions tag
	MkfsOptions = "mkfsOptions"
	// DiskTagedByPlugin tag
	DiskTagedByPlugin = "DISK_TAGED_BY_PLUGIN"
	// DiskMetricByPlugin tag
	DiskMetricByPlugin = "DISK_METRIC_BY_PLUGIN"
	// DiskDetachDisable tag
	DiskDetachDisable = "DISK_DETACH_DISABLE"
	// DiskBdfEnable tag
	DiskBdfEnable = "DISK_BDF_ENABLE"
	// DiskDetachBeforeDelete tag
	DiskDetachBeforeDelete = "DISK_DETACH_BEFORE_DELETE"
	// DiskAttachByController tag
	DiskAttachByController = "DISK_AD_CONTROLLER"
	// DiskAttachedKey attached key
	DiskAttachedKey = "k8s.aliyun.com"
	// DiskAttachedValue attached value
	DiskAttachedValue = "true"
	// VolumeDir volume dir
	VolumeDir = "/host/etc/kubernetes/volumes/disk/"
	// VolumeDirRemove volume dir remove
	VolumeDirRemove = "/host/etc/kubernetes/volumes/disk/remove"
	// MixRunTimeMode support both runc and runv
	MixRunTimeMode = "runc-runv"
	// RunvRunTimeMode tag
	RunvRunTimeMode = "runv"
	// InputOutputErr tag
	InputOutputErr = "input/output error"
	// BLOCKVOLUMEPREFIX block volume mount prefix
	BLOCKVOLUMEPREFIX = "/var/lib/kubelet/plugins/kubernetes.io/csi/volumeDevices/publish"
)

// QueryResponse response struct for query server
type QueryResponse struct {
	device     string
	volumeType string
	identity   string
	mountfile  string
	runtime    string
}

// NewNodeServer creates node server
func NewNodeServer(d *csicommon.CSIDriver, c *ecs.Client) csi.NodeServer {
	var maxVolumesNum int64 = 15
	volumeNum := os.Getenv("MAX_VOLUMES_PERNODE")
	if "" != volumeNum {
		num, err := strconv.ParseInt(volumeNum, 10, 64)
		if err != nil {
			log.Fatalf(log.TypeDisk, log.StatusInternalError, fmt.Sprintf("MAX_VOLUMES_PERNODE must be int64, but get: %s", volumeNum))
		} else {
			if num < 0 || num > 15 {
				log.Errorf(log.TypeDisk, log.StatusInternalError, fmt.Sprintf("MAX_VOLUMES_PERNODE must between 0-15, but get: %s", volumeNum))
			} else {
				maxVolumesNum = num
				log.Infof(log.TypeDisk, log.StatusOK, fmt.Sprintf("MAX_VOLUMES_PERNODE is set to(not default): %d", maxVolumesNum))
			}
		}
	} else {
		log.Infof(log.TypeDisk, log.StatusOK, fmt.Sprintf("MAX_VOLUMES_PERNODE is set to(default): %d", maxVolumesNum))
	}

	doc, err := getInstanceDoc()
	if err != nil {
		log.Fatalf(log.TypeDisk, log.StatusInternalError, fmt.Sprintf("Error happens to get node document: %v", err))
	}

	cfg, err := clientcmd.BuildConfigFromFlags(masterURL, kubeconfig)
	if err != nil {
		log.Fatalf(log.TypeDisk, log.StatusUnauthenticated, fmt.Sprintf("Get cluster config is failed, err:%s,", err.Error()))
	}
	kubeClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		log.Fatalf(log.TypeDisk, log.StatusUnauthenticated, fmt.Sprintf("Get cluster clientset is failed, err:%s,", err.Error()))
	}

	// Create Directory
	os.MkdirAll(VolumeDir, os.FileMode(0755))
	os.MkdirAll(VolumeDirRemove, os.FileMode(0755))

	if IsVFNode() {
		log.Infof(log.TypeDisk, log.StatusOK, "Currently node is VF model")
	} else {
		log.Infof(log.TypeDisk, log.StatusOK, "Currently node is NOT VF model")
	}

	return &nodeServer{
		zone:              doc.ZoneID,
		maxVolumesPerNode: maxVolumesNum,
		nodeID:            doc.InstanceID,
		DefaultNodeServer: csicommon.NewDefaultNodeServer(d),
		mounter:           utils.NewMounter(),
		k8smounter:        k8smount.New(""),
		clientSet:         kubeClient,
	}
}

func (ns *nodeServer) NodeGetCapabilities(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	// currently there is a single NodeServer capability according to the spec
	nscap := &csi.NodeServiceCapability{
		Type: &csi.NodeServiceCapability_Rpc{
			Rpc: &csi.NodeServiceCapability_RPC{
				Type: csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
			},
		},
	}
	nscap2 := &csi.NodeServiceCapability{
		Type: &csi.NodeServiceCapability_Rpc{
			Rpc: &csi.NodeServiceCapability_RPC{
				Type: csi.NodeServiceCapability_RPC_EXPAND_VOLUME,
			},
		},
	}
	nscap3 := &csi.NodeServiceCapability{
		Type: &csi.NodeServiceCapability_Rpc{
			Rpc: &csi.NodeServiceCapability_RPC{
				Type: csi.NodeServiceCapability_RPC_GET_VOLUME_STATS,
			},
		},
	}

	// Disk Metric enable config
	nodeSvcCap := []*csi.NodeServiceCapability{nscap, nscap2}
	if GlobalConfigVar.MetricEnable {
		nodeSvcCap = []*csi.NodeServiceCapability{nscap, nscap2, nscap3}
	}

	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: nodeSvcCap,
	}, nil
}

// csi disk driver: bind directory from global to pod.
func (ns *nodeServer) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	// check target mount path
	sourcePath := req.StagingTargetPath
	// running in runc/runv mode
	if GlobalConfigVar.RunTimeClass == MixRunTimeMode {
		if runtime, err := utils.GetPodRunTime(req, ns.clientSet); err != nil {
			errMsg := fmt.Sprintf("Can not get pod runtime: %s", err.Error())
			log.Errorf(log.TypeDisk, log.StatusInternalError, errMsg)
			return nil, status.Error(codes.Internal, errMsg)
		} else if runtime == RunvRunTimeMode {
			log.Infof(log.TypeDisk, log.StatusOK, fmt.Sprintf("Kata Disk Volume %s Mount with: %v", req.VolumeId, req))
			// umount the stage path, which is mounted in Stage
			if err := ns.unmountStageTarget(sourcePath); err != nil {
				errMsg := fmt.Sprintf("UnMountStageTarget %s with error: %s", sourcePath, err.Error())
				log.Errorf(log.TypeDisk, log.StatusUmountFailed, errMsg)
				return nil, status.Error(codes.InvalidArgument, errMsg)
			}
			deviceName, err := GetDeviceByVolumeID(req.VolumeId)
			if err != nil && deviceName == "" {
				deviceName = getVolumeConfig(req.VolumeId)
			}
			if deviceName == "" {
				errMsg := fmt.Sprintf("Can not get local deviceName for volume: %s", req.VolumeId)
				log.Errorf(log.TypeDisk, log.StatusInternalError, errMsg)
				return nil, status.Error(codes.InvalidArgument, errMsg)
			}

			// save volume info to local file
			mountFile := filepath.Join(req.GetTargetPath(), utils.CsiPluginRunTimeFlagFile)
			if err := utils.CreateDest(req.GetTargetPath()); err != nil {
				errMsg := fmt.Sprintf("Create path %s with error: %s", req.GetTargetPath(), err.Error())
				log.Errorf(log.TypeDisk, log.StatusInternalError, errMsg)
				return nil, status.Error(codes.InvalidArgument, errMsg)
			}

			qResponse := QueryResponse{}
			qResponse.device = deviceName
			qResponse.identity = req.GetTargetPath()
			qResponse.volumeType = "block"
			qResponse.mountfile = mountFile
			qResponse.runtime = RunvRunTimeMode
			if err := utils.WriteJosnFile(qResponse, mountFile); err != nil {
				errMsg := fmt.Sprintf("Write json file %s with err:%s", mountFile, err.Error())
				log.Errorf(log.TypeDisk, log.StatusInternalError, errMsg)
				return nil, status.Error(codes.InvalidArgument, errMsg)
			}

			log.Infof(log.TypeDisk, "Kata Disk Volume %s Mount Successful", req.VolumeId)
			return &csi.NodePublishVolumeResponse{}, nil
		}
	}

	isBlock := req.GetVolumeCapability().GetBlock() != nil
	if isBlock {
		sourcePath = filepath.Join(req.StagingTargetPath, req.VolumeId)
	}
	targetPath := req.GetTargetPath()
	log.Infof(log.TypeDisk, log.StatusOK, fmt.Sprintf("Starting Mount Volume %s, source %s > target %s", req.VolumeId, sourcePath, targetPath))
	if req.VolumeId == "" {
		errMsg := "Volume ID must be provided."
		log.Errorf(log.TypeDisk, log.StatusNotFound, errMsg)
		return nil, status.Error(codes.InvalidArgument, errMsg)
	}
	if req.StagingTargetPath == "" {
		errMsg := "Staging Target Path must be provided"
		log.Errorf(log.TypeDisk, log.StatusNotFound, errMsg)
		return nil, status.Error(codes.InvalidArgument, errMsg)
	}
	if req.VolumeCapability == nil {
		errMsg := "Volume Capability must be provided"
		log.Errorf(log.TypeDisk, log.StatusNotFound, errMsg)
		return nil, status.Error(codes.InvalidArgument, errMsg)
	}
	// check if block volume
	if isBlock {
		if !utils.IsMounted(targetPath) {
			if err := ns.mounter.EnsureBlock(targetPath); err != nil {
				errMsg := fmt.Sprintf("Create targetPath %s is failed, err:%s", targetPath, err.Error())
				log.Errorf(log.TypeDisk, log.StatusInternalError, errMsg)
				return nil, status.Error(codes.Internal, errMsg)
			}
			options := []string{"bind"}
			if err := ns.mounter.MountBlock(sourcePath, targetPath, options...); err != nil {
				return nil, err
			}
		}
		log.Infof(log.TypeDisk, log.StatusOK, fmt.Sprintf("Mount disk %s, from source %s to target %v", req.VolumeId, sourcePath, targetPath))
		return &csi.NodePublishVolumeResponse{}, nil
	}

	if !strings.HasSuffix(targetPath, "/mount") {
		errMsg := fmt.Sprintf("Volume %s malformed the value of target path: %s", req.VolumeId, targetPath)
		log.Errorf(log.TypeDisk, log.StatusInternalError, errMsg)
		return nil, status.Error(codes.InvalidArgument, errMsg)
	}
	if err := ns.mounter.EnsureFolder(targetPath); err != nil {
		errMsg := fmt.Sprintf("Create targetPath %s is failed, err:%s", targetPath, err.Error())
		log.Errorf(log.TypeDisk, log.StatusNotFound, errMsg)
		return nil, status.Error(codes.Internal, errMsg)
	}
	notmounted, err := ns.k8smounter.IsLikelyNotMountPoint(targetPath)
	if err != nil {
		errMsg := fmt.Sprintf("Path %s is not mount,err:%s", targetPath, err.Error())
		log.Errorf(log.TypeDisk, log.StatusMountFailed, errMsg)
		return nil, status.Error(codes.Internal, errMsg)
	}
	if !notmounted {
		log.Infof(log.TypeDisk, log.StatusMountFailed, fmt.Sprintf("VolumeId: %s, Path %s is already mounted", req.VolumeId, targetPath))
		return &csi.NodePublishVolumeResponse{}, nil
	}

	sourceNotMounted, err := ns.k8smounter.IsLikelyNotMountPoint(sourcePath)
	if err != nil {
		errMsg := fmt.Sprintf("Path %s is not likely mount,err:%s", targetPath, err.Error())
		log.Errorf(log.TypeDisk, log.StatusMountFailed, errMsg)
		return nil, status.Error(codes.Internal, errMsg)
	}
	if sourceNotMounted {
		device, _ := GetDeviceByVolumeID(req.GetVolumeId())
		if device != "" {
			if err := ns.mountDeviceToGlobal(req.VolumeCapability, req.VolumeContext, device, sourcePath); err != nil {
				errMsg := fmt.Sprintf("VolumeId: %s, remount disk to global %s error: %s", req.VolumeId, sourcePath, err.Error())
				log.Errorf(log.TypeDisk, log.StatusMountFailed, errMsg)
				return nil, status.Error(codes.Internal, errMsg)
			}
			log.Infof(log.TypeDisk, log.StatusMountFailed, fmt.Sprintf("SourcePath %s not mounted, and mounted again with device %s", sourcePath, device))
		} else {
			errMsg := fmt.Sprintf("VolumeId: %s, sourcePath %s is Not mounted and device cannot found", req.VolumeId, sourcePath)
			log.Errorf(log.TypeDisk, log.StatusMountFailed, errMsg)
			return nil, status.Error(codes.Internal, errMsg)
		}
	}

	// start to mount
	mnt := req.VolumeCapability.GetMount()
	options := append(mnt.MountFlags, "bind")
	if req.Readonly {
		options = append(options, "ro")
	}
	fsType := "ext4"
	if mnt.FsType != "" {
		fsType = mnt.FsType
	}

	// check device name available
	expectName := GetVolumeDeviceName(req.VolumeId)
	realDevice := GetDeviceByMntPoint(sourcePath)
	if realDevice == "" {
		opts := append(mnt.MountFlags, "shared")
		if err := ns.k8smounter.Mount(expectName, sourcePath, fsType, opts); err != nil {
			errMsg := fmt.Sprintf("Mount source path %s is failed, err:%s", sourcePath, err.Error())
			log.Errorf(log.TypeDisk, log.StatusMountFailed, errMsg)
			return nil, status.Error(codes.Internal, errMsg)
		}
		realDevice = GetDeviceByMntPoint(sourcePath)
	}
	if expectName != realDevice || realDevice == "" {
		errMsg := fmt.Sprintf("Volume: %s, sourcePath: %s real Device: %s not same with expected: %s", req.VolumeId, sourcePath, realDevice, expectName)
		log.Errorf(log.TypeDisk, log.StatusMountFailed, errMsg)
		return nil, status.Error(codes.Internal, errMsg)
	}

	log.Infof(log.TypeDisk, log.StatusOK, fmt.Sprintf("Starting mount volume %s with flags %v and fsType %s", req.VolumeId, options, fsType))
	if err = ns.k8smounter.Mount(sourcePath, targetPath, fsType, options); err != nil {
		errMsg := fmt.Sprintf("Mount sourcePath %s to targetPath %s is failed, err:%s", sourcePath, targetPath, err.Error())
		log.Errorf(log.TypeDisk, log.StatusMountFailed, errMsg)
		return nil, status.Error(codes.Internal, errMsg)
	}

	log.Infof(log.TypeDisk, log.StatusOK, fmt.Sprintf("Mount  Volume: %s, from source %s to target %v is successfully.", req.VolumeId, sourcePath, targetPath))
	return &csi.NodePublishVolumeResponse{}, nil
}

func (ns *nodeServer) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	targetPath := req.GetTargetPath()
	// Step 1: check folder exists
	if !IsFileExisting(targetPath) {
		if err := ns.unmountDuplicateMountPoint(targetPath); err != nil {
			errMsg := fmt.Sprintf("Umount MountPoint %s is failed, err:%s", targetPath, err.Error())
			log.Errorf(log.TypeDisk, log.StatusUmountFailed, errMsg)
			return nil, errors.New(errMsg)
		}
		log.Infof(log.TypeDisk, log.StatusInternalError, fmt.Sprintf("NodeUnpublishVolume: Volume %s Folder %s doesn't exist", req.VolumeId, targetPath))
		return &csi.NodeUnpublishVolumeResponse{}, nil
	}

	// check runtime mode
	if GlobalConfigVar.RunTimeClass == MixRunTimeMode && utils.IsMountPointRunv(targetPath) {
		fileName := filepath.Join(targetPath, utils.CsiPluginRunTimeFlagFile)
		if err := os.Remove(fileName); err != nil {
			errMsg := fmt.Sprintf("Remove Runv File %s with error: %s", fileName, err.Error())
			log.Errorf(log.TypeDisk, log.StatusInternalError, errMsg)
			return nil, status.Error(codes.Internal, errMsg)
		}
		log.Infof(log.TypeDisk, log.StatusOK, fmt.Sprintf("Remove runv file %s is Successfully", fileName))
		return &csi.NodeUnpublishVolumeResponse{}, nil
	}
	// Step 2: check mount point
	notmounted, err := ns.k8smounter.IsLikelyNotMountPoint(targetPath)
	if err != nil {
		errMsg := fmt.Sprintf("Path %s is not likely mount,err:%s", targetPath, err.Error())
		log.Errorf(log.TypeDisk, log.StatusMountFailed, errMsg)
		return nil, status.Error(codes.InvalidArgument, errMsg)
	}
	if notmounted {
		if empty, _ := IsDirEmpty(targetPath); empty {
			if err := ns.unmountDuplicateMountPoint(targetPath); err != nil {
				errMsg := fmt.Sprintf("Umount path %s is failed,err:%s", targetPath, err.Error())
				log.Errorf(log.TypeDisk, log.StatusUmountFailed, errMsg)
				return nil, status.Error(codes.Internal, errMsg)
			}
			log.Infof(log.TypeDisk, log.StatusInternalError, fmt.Sprintf("Path %s is unmounted and empty", targetPath))
			return &csi.NodeUnpublishVolumeResponse{}, nil
		}
		// Block device
		if !utils.IsDir(targetPath) && strings.HasPrefix(targetPath, BLOCKVOLUMEPREFIX) {
			if removeErr := os.Remove(targetPath); removeErr != nil {
				errMsg := fmt.Sprintf("Could not remove mount block target %s: %v", targetPath, removeErr)
				log.Errorf(log.TypeDisk, log.StatusInternalError, errMsg)
				return nil, status.Error(codes.Internal, errMsg)
			}
			return &csi.NodeUnpublishVolumeResponse{}, nil
		}
		errMsg := fmt.Sprintf("VolumeId: %s, Path %s is unmounted, but not empty dir", req.VolumeId, targetPath)
		log.Errorf(log.TypeDisk, log.StatusInternalError, errMsg)
		return nil, errors.New(errMsg)
	}

	// Step 3: umount target path
	err = ns.k8smounter.Unmount(targetPath)
	if err != nil {
		errMsg := fmt.Sprintf("volumeID: %s, umount path: %s with error: %s", req.VolumeId, targetPath, err.Error())
		log.Errorf(log.TypeDisk, log.StatusUmountFailed, errMsg)
		return nil, status.Errorf(codes.Internal, errMsg)
	}
	if utils.IsMounted(targetPath) {
		errMsg := fmt.Sprintf("TargetPath mounted yet: volumeId: %s with target %s", req.VolumeId, targetPath)
		log.Errorf(log.TypeDisk, log.StatusMountFailed, errMsg)
		return nil, status.Errorf(codes.Internal, errMsg)
	}

	// below directory can not be umounted by kubelet in ack
	if err := ns.unmountDuplicateMountPoint(targetPath); err != nil {
		errMsg := fmt.Sprintf("UnMount MountPoint %s is failed,err:%s", targetPath, err.Error())
		log.Errorf(log.TypeDisk, log.StatusUmountFailed, errMsg)
		return nil, status.Errorf(codes.Internal, errMsg)
	}

	log.Infof(log.TypeDisk, log.StatusOK, fmt.Sprintf("Volume %s, target %v, is umounted successfully.", req.VolumeId, targetPath))
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (ns *nodeServer) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	// Step 1: check input parameters
	targetPath := req.StagingTargetPath
	if req.VolumeId == "" {
		errMsg := "Volume ID must be provided"
		log.Errorf(log.TypeDisk, log.StatusInternalError, errMsg)
		return nil, status.Error(codes.InvalidArgument, errMsg)
	}
	// targetPath format: /var/lib/kubelet/plugins/kubernetes.io/csi/pv/pv-disk-1e7001e0-c54a-11e9-8f89-00163e0e78a0/globalmount
	if targetPath == "" {
		errMsg := "Staging Target Path must be provided"
		log.Errorf(log.TypeDisk, log.StatusInternalError, errMsg)
		return nil, status.Error(codes.InvalidArgument, errMsg)
	}
	if req.VolumeCapability == nil {
		errMsg := "Volume Capability must be provided"
		log.Errorf(log.TypeDisk, log.StatusInternalError, errMsg)
		return nil, status.Error(codes.InvalidArgument, errMsg)
	}

	isBlock := req.GetVolumeCapability().GetBlock() != nil
	if isBlock {
		targetPath = filepath.Join(targetPath, req.VolumeId)
		if utils.IsMounted(targetPath) {
			log.Infof(log.TypeDisk, log.StatusMountFailed, fmt.Sprintf("Block Already Mounted: volumeId: %s target %s", req.VolumeId, targetPath))
			return &csi.NodeStageVolumeResponse{}, nil
		}
		if err := ns.mounter.EnsureBlock(targetPath); err != nil {
			errMsg := fmt.Sprintf("Create targetPath %s is failed, err:%s", targetPath, err.Error())
			log.Errorf(log.TypeDisk, log.StatusInternalError, errMsg)
			return nil, status.Error(codes.Internal, errMsg)
		}
	} else {
		if err := ns.mounter.EnsureFolder(targetPath); err != nil {
			errMsg := fmt.Sprintf("Create targetPath %s is failed, err:%s", targetPath, err.Error())
			log.Errorf(log.TypeDisk, log.StatusNotFound, errMsg)
			return nil, status.Error(codes.Internal, errMsg)
		}
	}

	// Step 2: check target path mounted
	notmounted, err := ns.k8smounter.IsLikelyNotMountPoint(targetPath)
	if err != nil {
		errMsg := fmt.Sprintf("Path %s is not likely mount,err:%s", targetPath, err.Error())
		log.Errorf(log.TypeDisk, log.StatusMountFailed, errMsg)
		return nil, status.Error(codes.Internal, errMsg)
	}
	if !notmounted {
		deviceName := GetDeviceByMntPoint(targetPath)
		if err := checkDeviceAvailable(deviceName, req.VolumeId, targetPath); err != nil {
			errMsg := fmt.Sprintf("MountPath is mounted %s, but check device available error: %s", targetPath, err.Error())
			log.Errorf(log.TypeDisk, log.StatusInternalError, errMsg)
			return nil, status.Error(codes.Internal, errMsg)
		}
		log.Infof(log.TypeDisk, log.StatusOK, fmt.Sprintf("VolumeId: %s, Path: %s is already mounted, device: %s", req.VolumeId, targetPath, deviceName))
		return &csi.NodeStageVolumeResponse{}, nil
	}

	device := ""
	isSharedDisk := false
	if value, ok := req.VolumeContext[SharedEnable]; ok {
		value = strings.ToLower(value)
		if value == "enable" || value == "true" || value == "yes" {
			isSharedDisk = true
		}
	}

	// Step 4 Attach volume
	if GlobalConfigVar.ADControllerEnable {
		var bdf string
		device, err = GetDeviceByVolumeID(req.GetVolumeId())
		if IsVFNode() && device == "" {
			if bdf, err = bindBdfDisk(req.GetVolumeId()); err != nil {
				if err := unbindBdfDisk(req.GetVolumeId()); err != nil {
					errMsg := fmt.Sprintf("Failed to detach bdf disk, err:%s", err.Error())
					log.Errorf(log.TypeDisk, log.StatusInternalError, errMsg)
					return nil, status.Errorf(codes.Aborted, errMsg)
				}
				errMsg := fmt.Sprintf("Failed to attach bdf disk, err:%s", err.Error())
				log.Errorf(log.TypeDisk, log.StatusAttachVolumeFailed, errMsg)
				return nil, status.Errorf(codes.Aborted, errMsg)
			}
			device, err = GetDeviceByVolumeID(req.GetVolumeId())
			if bdf != "" && device == "" {
				device, err = GetDeviceByBdf(bdf)
			}
		}
		if err != nil {
			errMsg := fmt.Sprintf("ADController Enabled, but device can't be found in node: %s, error: %s", req.VolumeId, err.Error())
			log.Errorf(log.TypeDisk, log.StatusNotFound, errMsg)
			return nil, status.Errorf(codes.Aborted, errMsg)
		}
	} else {
		device, err = attachDisk(req.GetVolumeId(), ns.nodeID, isSharedDisk)
		if err != nil {
			fullErrorMessage := utils.FindSuggestionByErrorMessage(err.Error(), utils.DiskAttachDetach)
			errMsg := fmt.Sprintf("Attach volume: %s with error: %s", req.VolumeId, fullErrorMessage)
			log.Errorf(log.TypeDisk, log.StatusAttachVolumeFailed, errMsg)
			return nil, status.Errorf(codes.Aborted, errMsg)
		}
	}

	if err := checkDeviceAvailable(device, req.VolumeId, targetPath); err != nil {
		errMsg := fmt.Sprintf("MountPath is mounted %s, but check device available error: %s", targetPath, err.Error())
		log.Errorf(log.TypeDisk, log.StatusInternalError, errMsg)
		return nil, status.Error(codes.Internal, errMsg)
	}
	if err := saveVolumeConfig(req.VolumeId, device); err != nil {
		errMsg := fmt.Sprintf("Save volume %s config is failed, err:%s", req.VolumeId, err.Error())
		log.Errorf(log.TypeDisk, log.StatusInternalError, errMsg)
		return nil, status.Error(codes.Aborted, errMsg)
	}
	log.Infof(log.TypeDisk, log.StatusOK, fmt.Sprintf("Volume %s attach to Node: %s, Device: %s is successfully.", req.VolumeId, ns.nodeID, device))

	// sysConfig
	if value, ok := req.VolumeContext[SysConfigTag]; ok {
		configList := strings.Split(strings.TrimSpace(value), ",")
		for _, configStr := range configList {
			keyValue := strings.Split(configStr, "=")
			if len(keyValue) == 2 {
				fileName := filepath.Join("/sys/block/", filepath.Base(device), keyValue[0])
				configCmd := "echo '" + keyValue[1] + "' > " + fileName
				if _, err := utils.Run(configCmd); err != nil {
					errMsg := fmt.Sprintf("Execute command %s is failed, err:%s", configCmd, err.Error())
					log.Errorf(log.TypeDisk, log.StatusExecuCmdFailed, errMsg)
					return nil, status.Error(codes.Aborted, errMsg)
				}
				errMsg := fmt.Sprintf("Volume %s execute command: %s is successfully.", req.VolumeId, configCmd)
				log.Infof(log.TypeDisk, log.StatusOK, errMsg)
			} else {
				errMsg := fmt.Sprintf("Volume Block System Config with format error: %s", configStr)
				log.Errorf(log.TypeDisk, log.StatusInternalError, errMsg)
				return nil, status.Error(codes.Aborted, errMsg)
			}
		}
	}

	// Block volume not need to format
	if isBlock {
		if utils.IsMounted(targetPath) {
			errMsg := fmt.Sprintf("MountPoint %s is exist.", targetPath)
			log.Errorf(log.TypeDisk, log.StatusMountFailed, errMsg)
			return &csi.NodeStageVolumeResponse{}, nil
		}
		options := []string{"bind"}
		if err := ns.mounter.MountBlock(device, targetPath, options...); err != nil {
			errMsg := fmt.Sprintf("MountPoint %s device %s is mounted failed, err:%s", targetPath, device)
			log.Errorf(log.TypeDisk, log.StatusMountFailed, errMsg)
			return nil, status.Error(codes.Internal, errMsg)
		}
		log.Infof(log.TypeDisk, log.StatusOK, fmt.Sprintf("Mount Device %s to %s with options: %v is successfully.", device, targetPath, options))
		return &csi.NodeStageVolumeResponse{}, nil
	}

	// Step 5 Start to format
	mnt := req.VolumeCapability.GetMount()
	options := append(mnt.MountFlags, "shared")
	fsType := "ext4"
	if mnt.FsType != "" {
		fsType = mnt.FsType
	}
	if err := ns.mounter.EnsureFolder(targetPath); err != nil {
		errMsg := fmt.Sprintf("Create targetPath %s is failed, err:%s", targetPath, err.Error())
		log.Errorf(log.TypeDisk, log.StatusNotFound, errMsg)
		return nil, status.Error(codes.Internal, errMsg)
	}

	// Set mkfs options for ext3, ext4
	mkfsOptions := make([]string, 0)
	if value, ok := req.VolumeContext[MkfsOptions]; ok {
		mkfsOptions = strings.Split(value, " ")
	}

	// do format-mount or mount
	diskMounter := &k8smount.SafeFormatAndMount{Interface: ns.k8smounter, Exec: utilexec.New()}
	if len(mkfsOptions) > 0 && (fsType == "ext4" || fsType == "ext3") {
		if err := formatAndMount(diskMounter, device, targetPath, fsType, mkfsOptions, options); err != nil {
			errMsg := fmt.Sprintf("FormatAndMount fail with mkfsOptions %s, %s, %s, %s, %s with error: %s", device, targetPath, fsType, mkfsOptions, options, err.Error())
			log.Errorf(log.TypeDisk, log.StatusMountFailed, errMsg)
			return nil, status.Error(codes.Internal, errMsg)
		}
	} else {
		if err := diskMounter.FormatAndMount(device, targetPath, fsType, options); err != nil {
			errMsg := fmt.Sprintf("Format block device %s is failed, path:%s, fsType: %s, options:%s, with error: %s", device, targetPath, fsType, options, err.Error())
			log.Errorf(log.TypeDisk, log.StatusMountFailed, errMsg)
			return nil, status.Error(codes.Internal, errMsg)
		}
	}

	log.Infof(log.TypeDisk, log.StatusOK, fmt.Sprintf("VolumeId:%s, target %v, device: %s with mkfsOptions: %v is mount successfully.", req.VolumeId, targetPath, device, mkfsOptions))
	return &csi.NodeStageVolumeResponse{}, nil
}

// target format: /var/lib/kubelet/plugins/kubernetes.io/csi/pv/pv-disk-1e7001e0-c54a-11e9-8f89-00163e0e78a0/globalmount
func (ns *nodeServer) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	if req.VolumeId == "" {
		errMsg := fmt.Sprintf("Volume ID must be provided")
		log.Errorf(log.TypeDisk, log.StatusInternalError, errMsg)
		return nil, status.Error(codes.InvalidArgument, errMsg)
	}
	if req.StagingTargetPath == "" {
		errMsg := fmt.Sprintf("Staging Target Path must be provided")
		log.Errorf(log.TypeDisk, log.StatusInternalError, errMsg)
		return nil, status.Error(codes.InvalidArgument, errMsg)
	}

	// check block device mountpoint
	targetPath := req.GetStagingTargetPath()
	tmpPath := filepath.Join(req.GetStagingTargetPath(), req.VolumeId)
	if IsFileExisting(tmpPath) {
		fileInfo, err := os.Lstat(tmpPath)
		if err != nil {
			if strings.Contains(strings.ToLower(err.Error()), InputOutputErr) {
				if err = isPathAvailiable(targetPath); err != nil {
					if err = utils.Umount(targetPath); err != nil {
						errMsg := fmt.Sprintf("Umount target %s with err: %v", targetPath, err)
						log.Errorf(log.TypeDisk, log.StatusUmountFailed, errMsg)
						return nil, status.Errorf(codes.InvalidArgument, errMsg)
					}
					log.Warningf(log.TypeDisk, log.StatusUmountFailed, fmt.Sprintf("Target path %s show input/output error: %v, umount it.", targetPath, err))
				}
			} else {
				errMsg := fmt.Sprintf("Lstat mountpoint: %s with error: %s", tmpPath, err.Error())
				log.Errorf(log.TypeDisk, log.StatusInternalError, errMsg)
				return nil, status.Error(codes.InvalidArgument, errMsg)
			}
		} else if (fileInfo.Mode() & os.ModeDevice) != 0 {
			targetPath = tmpPath
		}
	}

	// Step 1: check folder exists and umount
	msgLog := ""
	if IsFileExisting(targetPath) {
		notmounted, err := ns.k8smounter.IsLikelyNotMountPoint(targetPath)
		if err != nil {
			errMsg := fmt.Sprintf("Path %s is not likely mount,err:%s", targetPath, err.Error())
			log.Errorf(log.TypeDisk, log.StatusMountFailed, errMsg)
			return nil, status.Error(codes.Internal, errMsg)
		}
		if !notmounted {
			err = ns.k8smounter.Unmount(targetPath)
			if err != nil {
				errMsg := fmt.Sprintf("volumeID %s, umount path: %s with error: %s", req.VolumeId, targetPath, err.Error())
				log.Errorf(log.TypeDisk, log.StatusUmountFailed, errMsg)
				return nil, status.Error(codes.Internal, errMsg)
			}
			if utils.IsMounted(targetPath) {
				errMsg := fmt.Sprintf("MountPoint %s is exist.", targetPath)
				log.Errorf(log.TypeDisk, log.StatusMountFailed, errMsg)
				return nil, status.Error(codes.Internal, errMsg)
			}
		} else {
			msgLog = fmt.Sprintf("VolumeId: %s, mountpoint: %s not mounted, skipping and continue to detach", req.VolumeId, targetPath)
		}
		// safe remove mountpoint
		err = ns.mounter.SafePathRemove(targetPath)
		if err != nil {
			errMsg := fmt.Sprintf("Delete path %s is failed, err:%s", targetPath, err.Error())
			log.Errorf(log.TypeDisk, log.StatusInternalError, errMsg)
			return nil, status.Error(codes.Internal, errMsg)
		}
	} else {
		msgLog = fmt.Sprintf("VolumeID %s, Path %s doesn't exist, continue to detach", req.VolumeId, targetPath)
	}

	if msgLog == "" {
		log.Infof(log.TypeDisk, log.StatusOK, fmt.Sprintf("VolumeID %s target %v is umount successfully.", targetPath, req.VolumeId))
	} else {
		log.Infof(log.TypeDisk, log.StatusUmountFailed, msgLog)
	}

	if IsVFNode() {
		if err := unbindBdfDisk(req.VolumeId); err != nil {
			errMsg := fmt.Sprintf("Unbind bdf tag is failed, err:%s", err.Error())
			log.Errorf(log.TypeDisk, log.StatusInternalError, errMsg)
			return nil, err
		}
	}

	// Do detach if ADController disable
	if !GlobalConfigVar.ADControllerEnable {
		// if DetachDisabled is set to true, return
		if GlobalConfigVar.DetachDisabled {
			log.Infof(log.TypeDisk, log.StatusInternalError, fmt.Sprintf("ADController is disable, detach flag set to false, PV %s", req.VolumeId))
			return &csi.NodeUnstageVolumeResponse{}, nil
		}

		err := detachDisk(req.VolumeId, ns.nodeID)
		if err != nil {
			errMsg := fmt.Sprintf("VolumeID: %s detach failed with error %v", req.VolumeId, err.Error())
			log.Errorf(log.TypeDisk, log.StatusDetachVolumeFailed, errMsg)
			return nil, errors.New(errMsg)
		}
		removeVolumeConfig(req.VolumeId)
	}

	return &csi.NodeUnstageVolumeResponse{}, nil
}

func (ns *nodeServer) NodeGetInfo(ctx context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{
		NodeId:            ns.nodeID,
		MaxVolumesPerNode: ns.maxVolumesPerNode,
		// make sure that the driver works on this particular zone only
		AccessibleTopology: &csi.Topology{
			Segments: map[string]string{
				TopologyZoneKey: ns.zone,
			},
		},
	}, nil
}

func (ns *nodeServer) NodeExpandVolume(ctx context.Context, req *csi.NodeExpandVolumeRequest) (
	*csi.NodeExpandVolumeResponse, error) {

	if len(req.GetVolumeId()) == 0 {
		errMsg := fmt.Sprintf("Volume ID is empty")
		log.Errorf(log.TypeDisk, log.StatusInternalError, errMsg)
		return nil, status.Error(codes.InvalidArgument, errMsg)
	}
	if len(req.GetVolumePath()) == 0 {
		errMsg := fmt.Sprintf("Volume path is empty")
		log.Errorf(log.TypeDisk, log.StatusInternalError, errMsg)
		return nil, status.Error(codes.InvalidArgument, errMsg)
	}

	volumePath := req.GetVolumePath()
	diskID := req.GetVolumeId()
	if strings.Contains(volumePath, BLOCKVOLUMEPREFIX) {
		log.Infof(log.TypeDisk, log.StatusInternalError, fmt.Sprintf("Block volume not expand fs, volumeId: %s, volumePath: %s", diskID, volumePath))
		return &csi.NodeExpandVolumeResponse{}, nil
	}

	devicePath := GetVolumeDeviceName(diskID)
	if devicePath == "" {
		errMsg := fmt.Sprintf("can't get devicePath: %s", diskID)
		log.Errorf(log.TypeDisk, log.StatusInternalError, errMsg)
		return nil, status.Error(codes.InvalidArgument, errMsg)
	}
	log.Infof(log.TypeDisk, log.StatusOK, fmt.Sprintf("VolumeID: %s, devicePath: %s, volumePath: %s", diskID, devicePath, volumePath))

	// use resizer to expand volume filesystem
	resizer := resizefs.NewResizeFs(&k8smount.SafeFormatAndMount{Interface: ns.k8smounter, Exec: utilexec.New()})
	ok, err := resizer.Resize(devicePath, volumePath)
	if err != nil {
		errMsg := fmt.Sprintf("Resize Error, volumeId: %s, devicePath: %s, volumePath: %s, err: %s", diskID, devicePath, volumePath, err.Error())
		log.Errorf(log.TypeDisk, log.StatusInternalError, errMsg)
		return nil, status.Error(codes.Internal, errMsg)
	}
	if !ok {
		errMsg := fmt.Sprintf("Resize failed, volumeId: %s, devicePath: %s, volumePath: %s", diskID, devicePath, volumePath)
		log.Errorf(log.TypeDisk, log.StatusInternalError, errMsg)
		return nil, status.Error(codes.Internal, errMsg)
	}
	log.Infof(log.TypeDisk, log.StatusOK, fmt.Sprintf("Resizefs volumeId: %s, devicePath: %s, volumePath: %s is successfully.", diskID, devicePath, volumePath))
	return &csi.NodeExpandVolumeResponse{}, nil
}

// NodeGetVolumeStats used for csi metrics
func (ns *nodeServer) NodeGetVolumeStats(ctx context.Context, req *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	targetPath := req.GetVolumePath()
	if targetPath == "" {
		errMsg := fmt.Sprintf("Path %v is empty", targetPath)
		log.Errorf(log.TypeDisk, log.StatusInternalError, errMsg)
		return nil, status.Error(codes.InvalidArgument, errMsg)
	}

	return utils.GetMetrics(targetPath)
}

// umount path and not remove
func (ns *nodeServer) unmountStageTarget(targetPath string) error {
	msgLog := "UnmountStageTarget: Unmount Stage Target: " + targetPath
	if IsFileExisting(targetPath) {
		notmounted, err := ns.k8smounter.IsLikelyNotMountPoint(targetPath)
		if err != nil {
			errMsg := fmt.Sprintf("Path %s is not mount,err:%s", targetPath, err.Error())
			log.Errorf(log.TypeDisk, log.StatusMountFailed, errMsg)
			return status.Error(codes.Internal, errMsg)
		}
		if !notmounted {
			err = ns.k8smounter.Unmount(targetPath)
			if err != nil {
				errMsg := fmt.Sprintf("Umount path: %s with error: %s", targetPath, err.Error())
				log.Errorf(log.TypeDisk, log.StatusUmountFailed, errMsg)
				return status.Error(codes.Internal, errMsg)
			}
		} else {
			msgLog = fmt.Sprintf("Umount %s is successfully.", targetPath)
		}
	} else {
		msgLog = fmt.Sprintf("Path %s doesn't exist.", targetPath)
	}

	log.Infof(log.TypeDisk, log.StatusOK, msgLog)
	return nil
}

func (ns *nodeServer) mountDeviceToGlobal(capability *csi.VolumeCapability, volumeContext map[string]string, device, sourcePath string) error {
	mnt := capability.GetMount()
	options := append(mnt.MountFlags, "shared")
	fsType := "ext4"
	if mnt.FsType != "" {
		fsType = mnt.FsType
	}
	if err := ns.mounter.EnsureFolder(sourcePath); err != nil {
		errMsg := fmt.Sprintf("Create targetPath %s is failed, err:%s", sourcePath, err.Error())
		log.Errorf(log.TypeDisk, log.StatusNotFound, errMsg)
		return status.Error(codes.Internal, errMsg)
	}

	// Set mkfs options for ext3, ext4
	mkfsOptions := make([]string, 0)
	if value, ok := volumeContext[MkfsOptions]; ok {
		mkfsOptions = strings.Split(value, " ")
	}

	// do format-mount or mount
	diskMounter := &k8smount.SafeFormatAndMount{Interface: ns.k8smounter, Exec: utilexec.New()}
	if len(mkfsOptions) > 0 && (fsType == "ext4" || fsType == "ext3") {
		if err := formatAndMount(diskMounter, device, sourcePath, fsType, mkfsOptions, options); err != nil {
			errMsg := fmt.Sprintf("FormatAndMount is failed, with mkfsOptions %s, %s, %s, %s, %s with error: %s", device, sourcePath, fsType, mkfsOptions, options, err.Error())
			log.Errorf(log.TypeDisk, log.StatusMountFailed, errMsg)
			return status.Error(codes.Internal, errMsg)
		}
	} else {
		if err := diskMounter.FormatAndMount(device, sourcePath, fsType, options); err != nil {
			errMsg := fmt.Sprintf("Format block device %s is failed, path:%s, fsType: %s, options:%s, with error: %s", device, sourcePath, fsType, options, err.Error())
			log.Errorf(log.TypeDisk, log.StatusMountFailed, errMsg)
			return status.Error(codes.Internal, errMsg)
		}
	}
	return nil
}

func (ns *nodeServer) unmountDuplicateMountPoint(targetPath string) error {
	pathParts := strings.Split(targetPath, "/")
	partsLen := len(pathParts)
	if partsLen > 2 && pathParts[partsLen-1] == "mount" {
		globalPath2 := filepath.Join("/var/lib/container/kubelet/plugins/kubernetes.io/csi/pv/", pathParts[partsLen-2], "/globalmount")
		if utils.IsFileExisting(globalPath2) {
			// check globalPath2 is mountpoint
			notmounted, err := ns.k8smounter.IsLikelyNotMountPoint(globalPath2)
			if err == nil && !notmounted {
				// check device is used by others
				refs, err := ns.k8smounter.GetMountRefs(globalPath2)
				if err == nil && !ns.mounter.HasMountRefs(globalPath2, refs) {
					errMsg := fmt.Sprintf("VolumeID unmount global path %s for ack with kubelet data disk", globalPath2)
					log.Errorf(log.TypeDisk, log.StatusUmountFailed, errMsg)
					if err := utils.Umount(globalPath2); err != nil {
						errMsg := fmt.Sprintf("Unmount global path %s failed with err: %v", globalPath2, err)
						log.Errorf(log.TypeDisk, log.StatusUmountFailed, errMsg)
						return status.Error(codes.Internal, errMsg)
					}
				} else {
					log.Infof(log.TypeDisk, log.StatusUmountFailed, fmt.Sprintf("Global path %s is mounted by others: %v", globalPath2, refs))
				}
			} else {
				log.Warningf(log.TypeDisk, log.StatusUmountFailed, fmt.Sprintf("Global path is not mounted: %s", globalPath2))
			}
		}
	} else {
		log.Warningf(log.TypeDisk, log.StatusUmountFailed, fmt.Sprintf("Target path is illegal format: %s", targetPath))
	}
	return nil
}
