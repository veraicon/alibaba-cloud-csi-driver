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
	"context"
	"errors"
	"fmt"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/golang/glog"
	"github.com/kubernetes-csi/drivers/pkg/csi-common"
	"github.com/kubernetes-sigs/alibaba-cloud-csi-driver/pkg/log"
	"github.com/kubernetes-sigs/alibaba-cloud-csi-driver/pkg/utils"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"path/filepath"
	"strings"
)

type nodeServer struct {
	*csicommon.DefaultNodeServer
}

// Options struct
type Options struct {
	Server     string `json:"server"`
	FileSystem string `json:"fileSystem"`
	SubPath    string `json:"subPath"`
	Options    string `json:"options"`
}

const (
	// CPFSTempMntPath used for create sub directory
	CPFSTempMntPath = "/mnt/acs_mnt/k8s_cpfs/temp"
)

func (ns *nodeServer) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	glog.Infof("NodePublishVolume:: CPFS Mount with: %s", req.VolumeContext)

	// parse parameters
	mountPath := req.GetTargetPath()
	opt := &Options{}
	for key, value := range req.VolumeContext {
		key = strings.ToLower(key)
		if key == "server" {
			opt.Server = value
		} else if key == "filesystem" {
			opt.FileSystem = value
		} else if key == "subpath" {
			opt.SubPath = value
		} else if key == "options" {
			opt.Options = value
		}
	}
	if mountPath == "" {
		return nil, errors.New("mountPath is empty")
	}
	if opt.Server == "" {
		return nil, errors.New("server is empty")
	}
	if opt.FileSystem == "" {
		return nil, errors.New("FileSystem is empty")
	}
	if opt.SubPath == "" {
		opt.SubPath = "/"
	}
	if !strings.HasPrefix(opt.SubPath, "/") {
		opt.SubPath = filepath.Join("/", opt.SubPath)
	}

	if utils.IsMounted(mountPath) {
		errMsg := fmt.Sprintf("MountPoint %s is exist.", mountPath)
		log.Errorf(log.TypeCPFS, log.StatusMountFailed, errMsg)
		return &csi.NodePublishVolumeResponse{}, nil
	}

	// Create Mount Path
	if err := utils.CreateDest(mountPath); err != nil {
		errMsg := fmt.Sprintf("Create path %s is failed, err:%s", mountPath, err.Error())
		log.Errorf(log.TypeCPFS, log.StatusInternalError, errMsg)
		return nil, errors.New(errMsg)
	}

	// Do mount
	mntCmd := fmt.Sprintf("mount -t lustre %s:/%s%s %s", opt.Server, opt.FileSystem, opt.SubPath, mountPath)
	if opt.Options != "" {
		mntCmd = fmt.Sprintf("mount -t lustre -o %s %s:/%s%s %s", opt.Options, opt.Server, opt.FileSystem, opt.SubPath, mountPath)
	}
	_, err := utils.Run(mntCmd)
	if err != nil && opt.SubPath != "/" && strings.Contains(err.Error(), "No such file or directory") {
		createCpfsSubDir(opt.Options, opt.Server, opt.FileSystem, opt.SubPath, req.VolumeId)
		if _, err := utils.Run(mntCmd); err != nil {
			errMsg := fmt.Sprintf("Execute command %s is failed, err:%s", mntCmd, err.Error())
			log.Errorf(log.TypeCPFS, log.StatusExecuCmdFailed, errMsg)
			return nil, errors.New(errMsg)
		}
	} else if err != nil {
		errMsg := fmt.Sprintf("Execute command %s is failed, err:%s", mntCmd, err.Error())
		log.Errorf(log.TypeCPFS, log.StatusExecuCmdFailed, errMsg)
		return nil, errors.New(errMsg)
	}

	// check mount
	if !utils.IsMounted(mountPath) {
		return nil, errors.New("Check mount fail after mount: " + mountPath)
	}
	log.Infof(log.TypeCPFS,log.StatusOK, "Mount success on mountpoint: %s, with Command: %s", mountPath, mntCmd)

	doCpfsConfig()
	return &csi.NodePublishVolumeResponse{}, nil
}

func doCpfsConfig() {
	configCmd := fmt.Sprintf("lctl set_param osc.*.max_rpcs_in_flight=128;lctl set_param osc.*.max_pages_per_rpc=256;lctl set_param mdc.*.max_rpcs_in_flight=256;lctl set_param mdc.*.max_mod_rpcs_in_flight=128")
	if _, err := utils.Run(configCmd); err != nil {
		errMsg := fmt.Sprintf("Execute command %s is failed, err:%s", configCmd, err.Error())
		log.Errorf(log.TypeCPFS, log.StatusExecuCmdFailed, errMsg)
	}
}

func (ns *nodeServer) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	mountPoint := req.TargetPath
	if !utils.IsMounted(mountPoint) {
		errMsg := fmt.Sprintf("MountPoint %s is Mounted", mountPoint)
		log.Errorf(log.TypeCPFS, log.StatusMountFailed, errMsg)
		return &csi.NodeUnpublishVolumeResponse{}, nil
	}

	umntCmd := fmt.Sprintf("umount %s", mountPoint)
	if _, err := utils.Run(umntCmd); err != nil {
		errMsg := fmt.Sprintf("Execute command %s is failed, err:%s", umntCmd, err.Error())
		log.Errorf(log.TypeCPFS, log.StatusExecuCmdFailed, errMsg)
		return nil, errors.New(errMsg)
	}
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (ns *nodeServer) NodeStageVolume(
	ctx context.Context,
	req *csi.NodeStageVolumeRequest) (
	*csi.NodeStageVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented,"Unimplemented NodeStageVolume function")
}

func (ns *nodeServer) NodeUnstageVolume(
	ctx context.Context,
	req *csi.NodeUnstageVolumeRequest) (
	*csi.NodeUnstageVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented,"Unimplemented NodeUnstageVolume function")
}

func (ns *nodeServer) NodeExpandVolume(ctx context.Context, req *csi.NodeExpandVolumeRequest) (
	*csi.NodeExpandVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented,"Unimplemented NodeExpandVolume function")
}
