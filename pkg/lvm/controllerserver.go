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

package lvm

import (
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-csi/drivers/pkg/csi-common"
	"github.com/kubernetes-sigs/alibaba-cloud-csi-driver/pkg/log"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type controllerServer struct {
	*csicommon.DefaultControllerServer
}

// newControllerServer creates a controllerServer object
func newControllerServer(d *csicommon.CSIDriver) *controllerServer {
	return &controllerServer{
		DefaultControllerServer: csicommon.NewDefaultControllerServer(d),
	}
}

func (cs *controllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	if err := cs.Driver.ValidateControllerServiceRequest(csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME); err != nil {
		log.Infof(log.TypeLVM, log.StatusOK, "CreateVolume:: Invalid create volume req: %v", req)
		return nil, err
	}
	if len(req.Name) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume Name cannot be empty")
	}
	if req.VolumeCapabilities == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume Capabilities cannot be empty")
	}

	volumeID := req.GetName()
	response := &csi.CreateVolumeResponse{}

	// Get nodeID if pvc in topology mode.
	nodeID := pickNodeID(req.GetAccessibilityRequirements())
	if nodeID == "" {
		response = &csi.CreateVolumeResponse{
			Volume: &csi.Volume{
				VolumeId:      volumeID,
				CapacityBytes: req.GetCapacityRange().GetRequiredBytes(),
				VolumeContext: req.GetParameters(),
			},
		}
	} else {
		response = &csi.CreateVolumeResponse{
			Volume: &csi.Volume{
				VolumeId:      volumeID,
				CapacityBytes: req.GetCapacityRange().GetRequiredBytes(),
				VolumeContext: req.GetParameters(),
				AccessibleTopology: []*csi.Topology{
					{
						Segments: map[string]string{
							TopologyNodeKey: nodeID,
						},
					},
				},
			},
		}
	}

	log.Infof(log.TypeLVM, log.StatusOK, "CreateVolume:: Success create volume: %s, volume size: %d", volumeID, req.GetCapacityRange().GetRequiredBytes())
	return response, nil
}

// pickNodeID selects node given topology requirement.
// if not found, empty string is returned.
func pickNodeID(requirement *csi.TopologyRequirement) string {
	if requirement == nil {
		return ""
	}
	for _, topology := range requirement.GetPreferred() {
		nodeID, exists := topology.GetSegments()[TopologyNodeKey]
		if exists {
			return nodeID
		}
	}
	for _, topology := range requirement.GetRequisite() {
		nodeID, exists := topology.GetSegments()[TopologyNodeKey]
		if exists {
			return nodeID
		}
	}
	return ""
}

func (cs *controllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	log.Infof(log.TypeLVM, log.StatusOK, "DeleteVolume:: Delete volume successfully: %s", req.GetVolumeId())
	return &csi.DeleteVolumeResponse{}, nil
}

func (cs *controllerServer) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	log.Infof(log.TypeLVM, log.StatusOK, "ControllerUnpublishVolume:: Do nothing by now: %s", req.VolumeId)
	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

func (cs *controllerServer) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	log.Infof(log.TypeLVM, log.StatusOK, "ControllerPublishVolume:: Do nothing by now: %s", req.VolumeId)
	return &csi.ControllerPublishVolumeResponse{}, nil
}

func (cs *controllerServer) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	log.Infof(log.TypeLVM, log.StatusOK, "ControllerExpandVolume:: %v", req)
	volSizeBytes := int64(req.GetCapacityRange().GetRequiredBytes())
	return &csi.ControllerExpandVolumeResponse{CapacityBytes: volSizeBytes, NodeExpansionRequired: true}, nil
}
