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

package mem

import (
	"errors"
	"fmt"
	"github.com/kubernetes-sigs/alibaba-cloud-csi-driver/pkg/log"
	"os"
	"strconv"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-csi/drivers/pkg/csi-common"
	"github.com/kubernetes-sigs/alibaba-cloud-csi-driver/pkg/utils"
	"golang.org/x/net/context"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	k8smount "k8s.io/utils/mount"
)

const (
	// NsenterCmd is the nsenter command
	NsenterCmd = "/nsenter --mount=/proc/1/ns/mnt"
	// NodeAffinity is the pv node schedule tag
	NodeAffinity = "nodeAffinity"
	// LocalDisk local disk
	LocalDisk = "localdisk"
	// DefaultFs default fs
	DefaultFs = "ext4"
)

type nodeServer struct {
	*csicommon.DefaultNodeServer
	nodeID     string
	mounter    utils.Mounter
	client     kubernetes.Interface
	k8smounter k8smount.Interface
}

var (
	masterURL  string
	kubeconfig string
)

// NewNodeServer create a NodeServer object
func NewNodeServer(d *csicommon.CSIDriver, nodeID string) csi.NodeServer {
	cfg, err := clientcmd.BuildConfigFromFlags(masterURL, kubeconfig)
	if err != nil {
		log.Fatalf(log.TypeMEM, log.StatusUnauthenticated, fmt.Sprintf("Get cluster config is failed, err:%s,", err.Error()))
	}

	kubeClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		log.Fatalf(log.TypeMEM, log.StatusUnauthenticated, fmt.Sprintf("Get cluster clientset is failed, err:%s,", err.Error()))
	}

	return &nodeServer{
		DefaultNodeServer: csicommon.NewDefaultNodeServer(d),
		nodeID:            nodeID,
		mounter:           utils.NewMounter(),
		k8smounter:        k8smount.New(""),
		client:            kubeClient,
	}
}

func (ns *nodeServer) GetNodeID() string {
	return ns.nodeID
}

func (ns *nodeServer) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	// parse request args.
	targetPath := req.GetTargetPath()
	if targetPath == "" {
		return nil, errors.New("targetPath is empty")
	}

	pvName := GetPvNameFormMntPoint(targetPath)
	if pvName == "" {
		return nil, errors.New("cannot parse pvName from targetPath: " + targetPath)
	}

	// Get pv size
	pv, err := ns.client.CoreV1().PersistentVolumes().Get(context.Background(), req.VolumeId, metav1.GetOptions{})
	if err != nil {
		log.Errorf(log.TypeMEM, log.StatusInternalError, fmt.Sprintf("Get pv %s is faile,err:%s", req.VolumeId, err.Error()))
		return nil, errors.New("targetPath is empty")
	}
	pvQuantity := pv.Spec.Capacity["storage"]
	pvSize := pvQuantity.Value()
	pvSizeNum := pvSize / (1024 * 1024 * 1024)
	pvSizeUnit := "g"
	if pvSizeNum == 0 {
		pvSizeNum = pvSize / (1024 * 1024)
		pvSizeUnit = "m"
	}
	if pvSizeNum == 0 {
		return nil, errors.New("get volume size with 0")
	}

	//mntCmd := fmt.Sprintf("mount -t tmpfs -o size=10G,nr_inodes=10k,mode=700 tmpfs /mytmpfs")
	options := []string{}
	for _, tmp := range pv.Spec.MountOptions {
		options = append(options, tmp)
	}
	pvSizeNumStr := strconv.FormatInt(pvSizeNum, 10)
	options = append(options, "size="+pvSizeNumStr+pvSizeUnit)
	if err := ns.k8smounter.Mount("tmpfs", targetPath, "tmpfs", options); err != nil {
		return nil, errors.New("Mount memory volume with err" + err.Error())
	}

	log.Infof(log.TypeMEM, log.StatusOK, fmt.Sprintf("Starting to mount memore at: %s, with sizeLimit: %s", targetPath, pvSizeNumStr+pvSizeUnit))

	return &csi.NodePublishVolumeResponse{}, nil
}

func (ns *nodeServer) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	targetPath := req.GetTargetPath()
	isMnt, err := ns.mounter.IsMounted(targetPath)
	if err != nil {
		if _, err := os.Stat(targetPath); os.IsNotExist(err) {
			return nil, errors.New("TargetPath not found")
		}
		return nil, err
	}
	if !isMnt {
		return &csi.NodeUnpublishVolumeResponse{}, nil
	}

	err = ns.mounter.Unmount(req.GetTargetPath())
	if err != nil {
		return nil, err
	}

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (ns *nodeServer) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	return &csi.NodeUnstageVolumeResponse{}, nil
}

func (ns *nodeServer) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	return &csi.NodeStageVolumeResponse{}, nil
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
	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: []*csi.NodeServiceCapability{
			nscap, nscap2,
		},
	}, nil
}

func (ns *nodeServer) NodeExpandVolume(ctx context.Context, req *csi.NodeExpandVolumeRequest) (
	*csi.NodeExpandVolumeResponse, error) {
	log.Infof(log.TypeMEM, log.StatusOK, fmt.Sprintf("Memory node expand volume: %v", req))
	return &csi.NodeExpandVolumeResponse{}, nil
}

func (ns *nodeServer) NodeGetInfo(ctx context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{
		NodeId: ns.nodeID,
		// make sure that the driver works on this particular node only
		AccessibleTopology: &csi.Topology{
			Segments: map[string]string{},
		},
	}, nil
}
