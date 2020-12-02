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

package cpfs

import (
	"errors"
	"fmt"
	"github.com/golang/glog"
	. "github.com/kubernetes-sigs/alibaba-cloud-csi-driver/pkg/logs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-csi/drivers/pkg/csi-common"
	"github.com/kubernetes-sigs/alibaba-cloud-csi-driver/pkg/utils"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// controller server try to create/delete volumes
type controllerServer struct {
	client kubernetes.Interface
	*csicommon.DefaultControllerServer
}

// resourcemode is selected by: subpath/filesystem
const (
	MNTROOTPATH = "/csi-persistentvolumes"
	MBSIZE      = 1024 * 1024
	DRIVER      = "driver"
	SERVER      = "server"
	MODE        = "mode"
	VOLUMEAS    = "volumeAs"
	PATH        = "path"
	SUBPATH     = "subpath"
	FILESYSTEM  = "filesystem"
)

// used by check pvc is processed
var pvcProcessSuccess = map[string]*csi.Volume{}
var storageClassServerPos = map[string]int{}

// NewControllerServer is to create controller server
func NewControllerServer(d *csicommon.CSIDriver) csi.ControllerServer {
	config, err := rest.InClusterConfig()
	if err != nil {
		glog.Fatal(GetLogInfoByErrorCode(StatusGetKubeConfigFailed, config.String(), err.Error()))
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		glog.Fatal(GetLogInfoByErrorCode(StatusGetKubeConfigFailed, config.String(), err.Error()))
	}

	c := &controllerServer{
		client:                  clientset,
		DefaultControllerServer: csicommon.NewDefaultControllerServer(d),
	}
	return c
}

// provisioner: create/delete cpfs volume
func (cs *controllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {

	// step1: check pvc is created or not.
	pvcUID := string(req.Name)
	if value, ok := pvcProcessSuccess[pvcUID]; ok {
		glog.Error(GetLogInfoByErrorCode(StatusCreateVolumeFailed, req.Name, "CPFS Volume has Created Already"))
		return &csi.CreateVolumeResponse{Volume: value}, nil
	}

	// parse cpfs parameters
	pvName := req.Name
	cpfsOptions := []string{}
	for _, volCap := range req.VolumeCapabilities {
		volCapMount := ((*volCap).AccessType).(*csi.VolumeCapability_Mount)
		for _, mountFlag := range volCapMount.Mount.MountFlags {
			cpfsOptions = append(cpfsOptions, mountFlag)
		}
	}
	cpfsOptionsStr := strings.Join(cpfsOptions, ",")
	cpfsServerInputs, cpfsServer, cpfsFileSystem, cpfsPath := "", "", "", ""

	// check provision mode
	volumeAs := "subpath"
	for k, v := range req.Parameters {
		switch strings.ToLower(k) {
		case VOLUMEAS:
			volumeAs = strings.TrimSpace(v)
		case SERVER:
			cpfsServerInputs = strings.TrimSpace(v)
		default:
		}
	}
	if volumeAs == "filesystem" {
		// TODO: support filesystem mode
		// CreateVolumeWithFileSystem()
		return nil, nil
	}

	// create pv with exist cpfs server
	cpfsServer, cpfsFileSystem, cpfsPath = GetCpfsDetails(cpfsServerInputs)
	if cpfsServer == "" || cpfsFileSystem == "" || cpfsPath == "" {
		glog.Error(GetLogInfoByErrorCode(StatusCreateVolumeFailed, req.Name, "Input cpfs server format error."))
		return nil, fmt.Errorf("CreateVolume: Input cpfs server format error: volume: %s, server: %s", req.Name, cpfsServerInputs)
	}

	// local mountpoint for one volume
	mountPoint := filepath.Join(MNTROOTPATH, pvName)
	if !utils.IsFileExisting(mountPoint) {
		if err := os.MkdirAll(mountPoint, 0777); err != nil {
			glog.Error(GetLogInfoByErrorCode(StatusCreateVolumeFailed, req.Name, fmt.Sprintf(" Unable to create directory: %s, with: %s", mountPoint, err.Error())))
			return nil, errors.New("Provision: " + req.Name + ", Unable to create directory: " + mountPoint + " with error: " + err.Error())
		}
	}

	// step5: Mount cpfs server to localpath
	if err := DoMount(cpfsServer, cpfsFileSystem, cpfsPath, cpfsOptionsStr, mountPoint, req.Name); err != nil {
		glog.Error(GetLogInfoByErrorCode(StatusCreateVolumeFailed, req.Name, fmt.Sprintf(" Mount server: %s, cpfsPath: %s, cpfsOptions: %s, mountPoint: %s, with: %s", cpfsServer, cpfsPath, cpfsOptionsStr, mountPoint, err.Error())))
		return nil, errors.New("CreateVolume: " + req.Name + ", Mount server: " + cpfsServer + ", cpfsPath: " + cpfsPath + ", cpfsOptions: " + cpfsOptionsStr + ", mountPoint: " + mountPoint + ", with error: " + err.Error())
	}

	// step6: create volume
	fullPath := filepath.Join(mountPoint, pvName)
	if err := os.MkdirAll(fullPath, 0777); err != nil {
		glog.Error(GetLogInfoByErrorCode(StatusCreateVolumeFailed, req.Name, fmt.Sprintf(" Create path: %s, with: %s", fullPath, err.Error())))
		return nil, errors.New("Provision: " + req.Name + ", creating path: " + fullPath + ", with error: " + err.Error())
	}
	os.Chmod(fullPath, 0777)

	// step7: Unmount cpfs server
	if err := utils.Umount(mountPoint); err != nil {
		glog.Error(GetLogInfoByErrorCode(StatusCreateVolumeFailed, req.Name, fmt.Sprintf(" Umount cpfs mountpoint %s  with : %s", fullPath, err.Error())))
		return nil, errors.New("unable to unmount cpfs server: " + cpfsServer)
	}

	volumeContext := map[string]string{}
	volumeContext["server"] = cpfsServer
	volumeContext["fileSystem"] = cpfsFileSystem
	volumeContext["subPath"] = filepath.Join(cpfsPath, pvName)
	volumeContext[VOLUMEAS] = SUBPATH
	if value, ok := req.Parameters["options"]; ok && value != "" {
		volumeContext["options"] = value
	}

	volSizeBytes := int64(req.GetCapacityRange().GetRequiredBytes())
	tmpVol := &csi.Volume{
		VolumeId:      req.Name,
		CapacityBytes: int64(volSizeBytes),
		VolumeContext: volumeContext,
	}
	pvcProcessSuccess[pvcUID] = tmpVol
	glog.Info(GetLogInfoByErrorCode(StatusCreateVolumeSuccess, req.Name, fmt.Sprintf("PV:%+v", tmpVol)))
	return &csi.CreateVolumeResponse{Volume: tmpVol}, nil
}

// Delete cpfs volume:
// support delete cpfs subpath: remove all files / rename path name;
// if stroageclass/archiveOnDelete: false, remove all files under the pv path;
// if stroageclass/archiveOnDelete: true, rename the pv path and all files saved;
func (cs *controllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	log.Infof("DeleteVolume: Starting to delete cpfs volume %s", req.GetVolumeId())
	pvPath, cpfsPath, cpfsFileSystem, cpfsServer, cpfsOptions, volumeAs := "", "", "", "", "", ""

	pvInfo, err := cs.client.CoreV1().PersistentVolumes().Get(context.Background(), req.VolumeId, metav1.GetOptions{})
	if err != nil {
		glog.Error(GetLogInfoByErrorCode(StatusGetPvFailed, req.VolumeId, err.Error()))
		return nil, fmt.Errorf("DeleteVolume: Get Volume: %s from cluster error: %s", req.VolumeId, err.Error())
	}
	cpfsOptions = strings.Join(pvInfo.Spec.MountOptions, ",")
	if pvInfo.Spec.CSI == nil {
		glog.Error(GetLogInfoByErrorCode(StatusPvSpecNotCSI, req.VolumeId))
		return nil, fmt.Errorf("DeleteVolume: Volume Spec with CSI empty: %s, pv: %v", req.VolumeId, pvInfo)
	}
	if value, ok := pvInfo.Spec.CSI.VolumeAttributes["server"]; ok {
		cpfsServer = value
	} else {
		glog.Error(GetLogInfoByErrorCode(StatusPvServerNotFound, req.VolumeId))
		return nil, fmt.Errorf("DeleteVolume: Volume Spec with cpfs server empty: %s, CSI: %v", req.VolumeId, pvInfo.Spec.CSI)
	}
	if value, ok := pvInfo.Spec.CSI.VolumeAttributes["fileSystem"]; ok {
		cpfsFileSystem = value
	} else {
		glog.Error(GetLogInfoByErrorCode(StatusPvFileSystemNotFound, req.VolumeId))
		return nil, fmt.Errorf("DeleteVolume: Volume Spec with cpfs filesystem empty: %s, CSI: %v", req.VolumeId, pvInfo.Spec.CSI)
	}
	if value, ok := pvInfo.Spec.CSI.VolumeAttributes["subPath"]; ok {
		pvPath = value
	} else {
		glog.Error(GetLogInfoByErrorCode(StatusPvSubPathNotFound, req.VolumeId))
		return nil, fmt.Errorf("DeleteVolume: Volume Spec with cpfs path empty: %s, CSI: %v", req.VolumeId, pvInfo.Spec.CSI)
	}
	if value, ok := pvInfo.Spec.CSI.VolumeAttributes[VOLUMEAS]; ok {
		volumeAs = value
	} else {
		volumeAs = SUBPATH
	}
	if volumeAs != SUBPATH {
		return nil, fmt.Errorf("DeleteVolume: dynamic PV only support subpath: %s", volumeAs)
	}

	if pvInfo.Spec.StorageClassName == "" {
		glog.Error(GetLogInfoByErrorCode(StatusPvStorageClassNotFound, req.VolumeId))
		return nil, fmt.Errorf("DeleteVolume: Volume Spec with storageclass empty: %s, Spec: %v", req.VolumeId, pvInfo.Spec)
	}
	storageclass, err := cs.client.StorageV1().StorageClasses().Get(context.Background(), pvInfo.Spec.StorageClassName, metav1.GetOptions{})
	if err != nil {
		glog.Error(GetLogInfoByErrorCode(StatusStorageClassNotFound, pvInfo.Spec.StorageClassName))
		return nil, fmt.Errorf("DeleteVolume: Volume: %s, reqeust storageclass error: %s", req.VolumeId, err.Error())
	}

	// parse cpfs mount point;
	tmpPath := pvPath
	strLen := len(pvPath)
	if pvPath[strLen-1:] == "/" {
		tmpPath = pvPath[0 : strLen-1]
	}
	pos := strings.LastIndex(tmpPath, "/")
	cpfsPath = pvPath[0:pos]
	if cpfsPath == "" {
		cpfsPath = "/"
	}

	// set the local mountpoint
	mountPoint := filepath.Join(MNTROOTPATH, req.VolumeId+"-delete")
	if err := DoMount(cpfsServer, cpfsFileSystem, cpfsPath, cpfsOptions, mountPoint, req.VolumeId); err != nil {
		glog.Error(GetLogInfoByErrorCode(StatusMountFailed, req.VolumeId, fmt.Sprintf("Mount server: %s, cpfsPath: %s, cpfsVersion: %s, cpfsOptions: %s, mountPoint: %s, with: %s", cpfsServer, cpfsPath, cpfsFileSystem, cpfsOptions, mountPoint, err.Error())))
		return nil, fmt.Errorf("DeleteVolume: %s, Mount server: %s, cpfsPath: %s, cpfsVersion: %s, cpfsOptions: %s, mountPoint: %s, with error: %s", req.VolumeId, cpfsServer, cpfsPath, cpfsFileSystem, cpfsOptions, mountPoint, err.Error())
	}
	defer utils.Umount(mountPoint)

	// remove the pvc process mapping if exist
	if _, ok := pvcProcessSuccess[req.VolumeId]; ok {
		delete(pvcProcessSuccess, req.VolumeId)
	}

	// pvName is same with volumeId
	pvName := filepath.Base(pvPath)
	deletePath := filepath.Join(mountPoint, pvName)
	if _, err := os.Stat(deletePath); os.IsNotExist(err) {
		glog.Info(GetLogInfoByErrorCode(StatusFileNotExist, deletePath))
		return &csi.DeleteVolumeResponse{}, nil
	}

	// Determine if the "archiveOnDelete" parameter exists.
	// If it exists and has a false value, delete the directory.
	// Otherwise, archive it.
	archiveOnDelete, exists := storageclass.Parameters["archiveOnDelete"]
	if exists {
		archiveBool, err := strconv.ParseBool(archiveOnDelete)
		if err != nil {
			return nil, errors.New("Check Mount cpfsserver fail " + cpfsServer + " error with: " + err.Error())
		}
		if !archiveBool {
			if err := os.RemoveAll(deletePath); err != nil {
				return nil, errors.New("Check Mount cpfsserver fail " + cpfsServer + " error with: " + err.Error())
			}
			return &csi.DeleteVolumeResponse{}, nil
		}
	}

	archivePath := filepath.Join(mountPoint, "archived-"+pvName+time.Now().Format(".2006-01-02-15:04:05"))
	if err := os.Rename(deletePath, archivePath); err != nil {
		glog.Error(GetLogInfoByErrorCode(StatusFileRenameFailed, deletePath, archivePath, err.Error()))
		return nil, errors.New("Check Mount cpfsserver fail " + cpfsServer + " error with: ")
	}

	return &csi.DeleteVolumeResponse{}, nil
}

func (cs *controllerServer) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	for _, cap := range req.VolumeCapabilities {
		if cap.GetAccessMode().GetMode() != csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER {
			return &csi.ValidateVolumeCapabilitiesResponse{Message: ""}, nil
		}
	}
	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeCapabilities: req.VolumeCapabilities,
		},
	}, nil
}

func (cs *controllerServer) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	glog.Infof("ControllerUnpublishVolume is called, do nothing by now")
	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

func (cs *controllerServer) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	glog.Infof("ControllerPublishVolume is called, do nothing by now")
	return &csi.ControllerPublishVolumeResponse{}, nil
}

//
func (cs *controllerServer) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	glog.Infof("CreateSnapshot is called, do nothing now")
	return &csi.CreateSnapshotResponse{}, nil
}

func (cs *controllerServer) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	glog.Infof("DeleteSnapshot is called, do nothing now")
	return &csi.DeleteSnapshotResponse{}, nil
}

func (cs *controllerServer) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest,
) (*csi.ControllerExpandVolumeResponse, error) {
	glog.Infof("ControllerExpandVolume is called, do nothing now")
	return &csi.ControllerExpandVolumeResponse{}, nil
}
