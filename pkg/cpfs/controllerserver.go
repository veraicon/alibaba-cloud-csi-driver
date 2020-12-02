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
	"github.com/kubernetes-sigs/alibaba-cloud-csi-driver/pkg/log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-csi/drivers/pkg/csi-common"
	"github.com/kubernetes-sigs/alibaba-cloud-csi-driver/pkg/utils"
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
		log.Fatalf(log.TypeCPFS, log.StatusUnauthenticated, "Get cluster config is failed, err:%s,", err.Error())
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalf(log.TypeCPFS, log.StatusUnauthenticated, "Get cluster clientset is failed, err:%s,", err.Error())
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
		log.Infof(log.TypeCPFS, log.StatusCreateVolumeFailed, "Cpfs volume %s has already created.", req.Name)
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
		errMsg := fmt.Sprintf("Get cpfs volume %s detail is empty.", req.Name)
		log.Errorf(log.TypeCPFS, log.StatusCreateVolumeFailed, errMsg)
		return nil, fmt.Errorf(errMsg)
	}

	// local mountpoint for one volume
	mountPoint := filepath.Join(MNTROOTPATH, pvName)
	if !utils.IsFileExisting(mountPoint) {
		if err := os.MkdirAll(mountPoint, 0777); err != nil {
			errMsg := fmt.Sprintf("DiskID %s unable to create directory:%s, with: %s", req.Name, mountPoint, err.Error())
			log.Errorf(log.TypeCPFS, log.StatusCreateVolumeFailed, errMsg)
			return nil, errors.New(errMsg)
		}
	}

	// step5: Mount cpfs server to localpath
	if err := DoMount(cpfsServer, cpfsFileSystem, cpfsPath, cpfsOptionsStr, mountPoint, req.Name); err != nil {
		errMsg := fmt.Sprintf("DiskID %s mount server: %s, cpfsPath: %s, cpfsOptions: %s, mountPoint: %s, with: %s", req.Name, cpfsServer, cpfsPath, cpfsOptionsStr, mountPoint, err.Error())
		log.Errorf(log.TypeCPFS, log.StatusMountFailed, errMsg)
		return nil, errors.New(errMsg)
	}

	// step6: create volume
	fullPath := filepath.Join(mountPoint, pvName)
	if err := os.MkdirAll(fullPath, 0777); err != nil {
		errMsg := fmt.Sprintf("DiskID %s create path %s is failed, err:%s", req.Name, fullPath, err.Error())
		log.Errorf(log.TypeCPFS, log.StatusCreateVolumeFailed, errMsg)
		return nil, errors.New(errMsg)
	}
	os.Chmod(fullPath, 0777)

	// step7: Unmount cpfs server
	if err := utils.Umount(mountPoint); err != nil {
		errMsg := fmt.Sprintf("DiskID %s umount cpfs mountpoint %s is failed, err:%s", req.Name, fullPath, err.Error())
		log.Errorf(log.TypeCPFS, log.StatusUmountFailed, errMsg)
		return nil, errors.New(errMsg)
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

	log.Infof(log.TypeCPFS, log.StatusOK, fmt.Sprintf("Create volume %s is successfully, PV: %+v", req.Name, tmpVol))
	return &csi.CreateVolumeResponse{Volume: tmpVol}, nil
}

// Delete cpfs volume:
// support delete cpfs subpath: remove all files / rename path name;
// if stroageclass/archiveOnDelete: false, remove all files under the pv path;
// if stroageclass/archiveOnDelete: true, rename the pv path and all files saved;
func (cs *controllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	pvPath, cpfsPath, cpfsFileSystem, cpfsServer, cpfsOptions, volumeAs := "", "", "", "", "", ""

	pvInfo, err := cs.client.CoreV1().PersistentVolumes().Get(context.Background(), req.VolumeId, metav1.GetOptions{})
	if err != nil {
		errMsg := fmt.Sprintf("Get volume %s from API-Server, err:%s", req.VolumeId, err.Error())
		log.Errorf(log.TypeCPFS, log.StatusDeleteVolumeFailed, errMsg)
		return nil, fmt.Errorf(errMsg)
	}
	cpfsOptions = strings.Join(pvInfo.Spec.MountOptions, ",")
	if pvInfo.Spec.CSI == nil {
		errMsg := fmt.Sprintf("Volume %s spec is not csi, pv:%s", req.VolumeId, pvInfo)
		log.Errorf(log.TypeCPFS, log.StatusDeleteVolumeFailed, errMsg)
		return nil, fmt.Errorf(errMsg)
	}
	if value, ok := pvInfo.Spec.CSI.VolumeAttributes["server"]; ok {
		cpfsServer = value
	} else {
		errMsg := fmt.Sprintf("Volume %s spec's server is empty", req.VolumeId)
		log.Errorf(log.TypeCPFS, log.StatusDeleteVolumeFailed, errMsg)
		return nil, fmt.Errorf(errMsg)
	}
	if value, ok := pvInfo.Spec.CSI.VolumeAttributes["fileSystem"]; ok {
		cpfsFileSystem = value
	} else {
		errMsg := fmt.Sprintf("Volume %s spec's filesystem is empty", req.VolumeId)
		log.Errorf(log.TypeCPFS, log.StatusDeleteVolumeFailed, errMsg)
		return nil, fmt.Errorf(errMsg)
	}
	if value, ok := pvInfo.Spec.CSI.VolumeAttributes["subPath"]; ok {
		pvPath = value
	} else {
		errMsg := fmt.Sprintf("Volume %s spec's subpath is empty", req.VolumeId)
		log.Errorf(log.TypeCPFS, log.StatusNotFound, errMsg)
		return nil, fmt.Errorf(errMsg)
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
		errMsg := fmt.Sprintf("Volume %s spec's storageclass is empty", req.VolumeId)
		log.Errorf(log.TypeCPFS, log.StatusNotFound, errMsg)
		return nil, fmt.Errorf(errMsg)
	}
	storageclass, err := cs.client.StorageV1().StorageClasses().Get(context.Background(), pvInfo.Spec.StorageClassName, metav1.GetOptions{})
	if err != nil {
		errMsg := fmt.Sprintf("Volume %s get storageclass %s failed, err:%s", req.VolumeId, pvInfo.Spec.StorageClassName, err)
		log.Errorf(log.TypeCPFS, log.StatusNotFound, errMsg)
		return nil, fmt.Errorf(errMsg)
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
		errMsg := fmt.Sprintf("VolumeID %s mount server: %s, cpfsPath: %s, cpfsVersion: %s, cpfsOptions: %s, mountPoint: %s, with: %s", req.VolumeId, cpfsServer, cpfsPath, cpfsFileSystem, cpfsOptions, mountPoint, err.Error())
		log.Errorf(log.TypeCPFS, log.StatusMountFailed, errMsg)
		return nil, fmt.Errorf(errMsg)
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
		errMsg := fmt.Sprintf("Stat path %s is not exist.", deletePath)
		log.Infof(log.TypeCPFS, log.StatusNotFound, errMsg)
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
		errMsg := fmt.Sprintf("Rename %s to %s is failed,err:%s", deletePath, archivePath, err.Error())
		return nil, errors.New(errMsg)
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
	log.Infof(log.TypeCPFS, log.StatusOK, "ControllerUnpublishVolume is called, do nothing by now")
	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

func (cs *controllerServer) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	log.Infof(log.TypeCPFS, log.StatusOK, "ControllerPublishVolume is called, do nothing by now")
	return &csi.ControllerPublishVolumeResponse{}, nil
}

//
func (cs *controllerServer) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	log.Infof(log.TypeCPFS, log.StatusOK, "CreateSnapshot is called, do nothing now")
	return &csi.CreateSnapshotResponse{}, nil
}

func (cs *controllerServer) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	log.Infof(log.TypeCPFS, log.StatusOK, "DeleteSnapshot is called, do nothing now")
	return &csi.DeleteSnapshotResponse{}, nil
}

func (cs *controllerServer) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest,
) (*csi.ControllerExpandVolumeResponse, error) {
	log.Infof(log.TypeCPFS, log.StatusOK, "ControllerExpandVolume is called, do nothing now")
	return &csi.ControllerExpandVolumeResponse{}, nil
}
