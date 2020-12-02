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
	"fmt"
	"github.com/kubernetes-sigs/alibaba-cloud-csi-driver/pkg/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"strconv"
	"strings"
	"time"

	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/requests"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/ecs"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/pkg/errors"
)

// attach alibaba cloud disk
func attachDisk(diskID, nodeID string, isSharedDisk bool) (string, error) {
	// Step 1: check disk status
	GlobalConfigVar.EcsClient = updateEcsClent(GlobalConfigVar.EcsClient)
	disk, err := findDiskByID(diskID)
	if err != nil {
		errMsg := fmt.Sprintf("Attach disk %s with err:%s", diskID, err.Error())
		log.Errorf(log.TypeDisk, log.StatusAttachVolumeFailed, errMsg)
		return "", status.Errorf(codes.Internal, errMsg)
	}
	if disk == nil {
		errMsg := fmt.Sprintf("Attach disk can't find disk %s ", diskID)
		log.Errorf(log.TypeDisk, log.StatusAttachVolumeFailed, errMsg)
		return "", status.Errorf(codes.Internal, errMsg)
	}

	if !GlobalConfigVar.ADControllerEnable {
		// NodeStageVolume should be called by sequence
		// In order no to block to caller, use boolean canAttach to check whether to continue.
		GlobalConfigVar.AttachMutex.Lock()
		if !GlobalConfigVar.CanAttach {
			GlobalConfigVar.AttachMutex.Unlock()
			errMsg := fmt.Sprintf("Provious disk is still attach processing,this DiskID %s", diskID)
			log.Errorf(log.TypeDisk, log.StatusAttachVolumeFailed, errMsg)
			return "", status.Error(codes.Aborted, errMsg)
		}
		GlobalConfigVar.CanAttach = false
		GlobalConfigVar.AttachMutex.Unlock()
		defer func() {
			GlobalConfigVar.AttachMutex.Lock()
			GlobalConfigVar.CanAttach = true
			GlobalConfigVar.AttachMutex.Unlock()
		}()
	}

	// tag disk as k8s.aliyun.com=true
	if GlobalConfigVar.DiskTagEnable {
		tagDiskAsK8sAttached(diskID)
	}

	if isSharedDisk {
		attachedToLocal := false
		for _, instance := range disk.MountInstances.MountInstance {
			if instance.InstanceId == nodeID {
				attachedToLocal = true
				break
			}
		}
		if attachedToLocal {
			detachRequest := ecs.CreateDetachDiskRequest()
			detachRequest.InstanceId = nodeID
			detachRequest.DiskId = diskID
			_, err = GlobalConfigVar.EcsClient.DetachDisk(detachRequest)
			if err != nil {
				errMsg := fmt.Sprintf("Can't attach disk %s to instance %s, err:%s", diskID, disk.InstanceId, err.Error())
				log.Errorf(log.TypeDisk, log.StatusAttachVolumeFailed, errMsg)
				return "", status.Errorf(codes.Aborted, errMsg)
			}

			if err := waitForSharedDiskInStatus(10, time.Second*3, diskID, nodeID, DiskStatusDetached); err != nil {
				return "", err
			}
		}
	} else {
		// detach disk first if attached
		if disk.Status == DiskStatusInuse {
			if disk.InstanceId == nodeID {
				if GlobalConfigVar.ADControllerEnable {
					errMsg := fmt.Sprintf("DiskID %s is repeat attach to intance %s", diskID, disk.InstanceId)
					log.Errorf(log.TypeDisk, log.StatusAttachVolumeFailed, errMsg)
					return "", nil
				}
				deviceName := GetVolumeDeviceName(diskID)
				if deviceName != "" && IsFileExisting(deviceName) {
					// TODO:
					if used, err := IsDeviceUsedOthers(deviceName, diskID); err == nil && used == false {
						errMsg := fmt.Sprintf("DiskID %s is used to other intance %s", diskID, disk.InstanceId)
						log.Errorf(log.TypeDisk, log.StatusAttachVolumeFailed, errMsg)
						return deviceName, nil
					}
				}
				// TODO: use uuid get devicepath
			}

			if GlobalConfigVar.DiskBdfEnable {
				if allowed, err := forceDetachAllowed(disk, nodeID); err != nil {
					err = errors.Wrapf(err, "forceDetachAllowed")
					errMsg := fmt.Sprintf("DiskID %s force detach allowed,err:%s", diskID, err.Error())
					log.Errorf(log.TypeDisk, log.StatusAttachVolumeFailed, errMsg)
					return "", status.Errorf(codes.Aborted, errMsg)
				} else if !allowed {
					errMsg := fmt.Sprintf("DiskID %s is already attach to instance %s.", diskID, disk.InstanceId)
					log.Errorf(log.TypeDisk, log.StatusAttachVolumeFailed, errMsg)
					return "", status.Errorf(codes.Aborted, errMsg)
				}
			}
			log.Infof(log.TypeDisk, log.StatusOK, fmt.Sprintf("Disk %s is already attached to instance %s, will be detached", diskID, disk.InstanceId))
			detachRequest := ecs.CreateDetachDiskRequest()
			detachRequest.InstanceId = disk.InstanceId
			detachRequest.DiskId = disk.DiskId
			_, err = GlobalConfigVar.EcsClient.DetachDisk(detachRequest)
			if err != nil {
				errMsg := fmt.Sprintf("Can't detach disk %s from instance %s: with error: %v", diskID, disk.InstanceId, err)
				log.Errorf(log.TypeDisk, log.StatusAttachVolumeFailed, errMsg)
				return "", status.Errorf(codes.Aborted, errMsg)
			}
		} else if disk.Status == DiskStatusAttaching {
			errMsg := fmt.Sprintf("DiskID %s is attaching to instace %s, err:%s", diskID, disk.InstanceId, err)
			log.Errorf(log.TypeDisk, log.StatusAttachVolumeFailed, errMsg)
			return "", status.Errorf(codes.Aborted, errMsg)
		}
		// Step 2: wait for Detach
		if disk.Status != DiskStatusAvailable {
			errMsg := fmt.Sprintf("DiskID %s wait detached finish.", diskID)
			log.Errorf(log.TypeDisk, log.StatusAttachVolumeFailed, errMsg)
			if err := waitForDiskInStatus(15, time.Second*3, diskID, DiskStatusAvailable); err != nil {
				return "", status.Errorf(codes.Aborted, errMsg)
			}
		}
	}

	// Step 3: Attach Disk, list device before attach disk
	before := []string{}
	if !GlobalConfigVar.ADControllerEnable {
		before = getDevices()
	}
	attachRequest := ecs.CreateAttachDiskRequest()
	attachRequest.InstanceId = nodeID
	attachRequest.DiskId = diskID
	response, err := GlobalConfigVar.EcsClient.AttachDisk(attachRequest)
	if err != nil {
		if strings.Contains(err.Error(), DiskLimitExceeded) {
			errMsg := fmt.Sprintf("The amount of the disk on instance in question reach its limits. Node %s exceed the limit attachments of disk, which at most 16 disks.", nodeID)
			log.Errorf(log.TypeDisk, log.StatusAttachVolumeFailed, errMsg)
			return "", status.Error(codes.Internal, errMsg)
		} else if strings.Contains(err.Error(), DiskNotPortable) {
			errMsg := fmt.Sprintf("The specified disk %s is not a portable disk.", diskID)
			log.Errorf(log.TypeDisk, log.StatusEcsApiErr, errMsg)
			return "", status.Error(codes.Internal, errMsg)
		}
		errMsg := fmt.Sprintf("DiskID  %s is attach failed, err:%s.", diskID, err.Error())
		log.Errorf(log.TypeDisk, log.StatusEcsApiErr, errMsg)
		return "", status.Errorf(codes.Aborted, errMsg)
	}

	// Step 4: wait for disk attached
	log.Infof(log.TypeDisk, log.StatusOK, fmt.Sprintf("Waiting for DiskID %s is attached to instance %s with requestID: %s", diskID, nodeID, response.RequestId))
	if isSharedDisk {
		if err := waitForSharedDiskInStatus(20, time.Second*3, diskID, nodeID, DiskStatusAttached); err != nil {
			return "", err
		}
	} else {
		if err := waitForDiskInStatus(20, time.Second*3, diskID, DiskStatusInuse); err != nil {
			return "", err
		}
	}

	// step 5: diff device with previous files under /dev
	if !GlobalConfigVar.ADControllerEnable {
		deviceName, _ := GetDeviceByVolumeID(diskID)
		if deviceName != "" {
			log.Infof(log.TypeDisk, log.StatusOK, fmt.Sprintf("Attach disk %s to node %s device %s is successfully.", diskID, nodeID, deviceName))
			return deviceName, nil
		}
		// TODO: use uuid get devicepath
		after := getDevices()
		devicePaths := calcNewDevices(before, after)

		// BDF Disk Logical
		if IsVFNode() && len(devicePaths) == 0 {
			var bdf string
			if bdf, err = bindBdfDisk(disk.DiskId); err != nil {
				if err := unbindBdfDisk(disk.DiskId); err != nil {
					errMsg := fmt.Sprintf("Unbind bdf tag is failed, err:%s", err.Error())
					log.Errorf(log.TypeDisk, log.StatusInternalError, errMsg)
					return "", status.Errorf(codes.Aborted, errMsg)
				}
				errMsg := fmt.Sprintf("Bind bdf tag is failed, err:%s", err.Error())
				log.Errorf(log.TypeDisk, log.StatusInternalError, errMsg)
				return "", status.Errorf(codes.Aborted, errMsg)
			}

			deviceName, err := GetDeviceByVolumeID(diskID)
			if len(deviceName) == 0 && bdf != "" {
				deviceName, err = GetDeviceByBdf(bdf)
			}
			if err == nil && deviceName != "" {
				log.Infof(log.TypeDisk, log.StatusOK, fmt.Sprintf("Attach disk %s to node %s device %s is successfully.", diskID, nodeID, deviceName))
				return deviceName, nil
			}
			after = getDevices()
			devicePaths = calcNewDevices(before, after)
		}

		if len(devicePaths) == 2 {
			if strings.HasPrefix(devicePaths[1], devicePaths[0]) {
				return devicePaths[1], nil
			} else if strings.HasPrefix(devicePaths[0], devicePaths[1]) {
				return devicePaths[0], nil
			}
		}
		if len(devicePaths) == 1 {
			log.Infof(log.TypeDisk, log.StatusOK, fmt.Sprintf("Attach disk %s to node %s device %s by diff is successfully.", diskID, nodeID, devicePaths[0]))
			return devicePaths[0], nil
		}
		// device count is not expected, should retry (later by detaching and attaching again)
		errMsg := fmt.Sprintf("Get Device Name error, with Before: %s, After: %s, diff: %s", before, after, devicePaths)
		log.Errorf(log.TypeDisk, log.StatusDetachVolumeFailed, errMsg)
		return "", status.Error(codes.Aborted, errMsg)
	}

	log.Infof(log.TypeDisk, log.StatusOK, fmt.Sprintf("Attach disk %s to node %s is successfully.", diskID, nodeID))
	return "", nil
}

func detachDisk(diskID, nodeID string) error {
	GlobalConfigVar.EcsClient = updateEcsClent(GlobalConfigVar.EcsClient)
	disk, err := findDiskByID(diskID)
	if err != nil {
		errMsg := fmt.Sprintf("DiskID %s is not found,err:%s", diskID, err.Error())
		log.Errorf(log.TypeDisk, log.StatusNotFound, errMsg)
		return status.Error(codes.Aborted, errMsg)
	}
	if disk == nil {
		errMsg := fmt.Sprintf("DiskID %s is not found,err:%s", diskID, err.Error())
		log.Errorf(log.TypeDisk, log.StatusNotFound, errMsg)
		return nil
	}
	beforeAttachTime := disk.AttachedTime

	if disk.InstanceId != "" {
		if disk.InstanceId == nodeID {
			// NodeStageVolume/NodeUnstageVolume should be called by sequence
			// In order no to block to caller, use boolean canAttach to check whether to continue.
			if !GlobalConfigVar.ADControllerEnable {
				GlobalConfigVar.AttachMutex.Lock()
				if !GlobalConfigVar.CanAttach {
					GlobalConfigVar.AttachMutex.Unlock()
					return status.Error(codes.Aborted, "Previous attach/detach action is still in process, volume: "+diskID)
				}
				GlobalConfigVar.CanAttach = false
				GlobalConfigVar.AttachMutex.Unlock()
				defer func() {
					GlobalConfigVar.AttachMutex.Lock()
					GlobalConfigVar.CanAttach = true
					GlobalConfigVar.AttachMutex.Unlock()
				}()
			}
			if GlobalConfigVar.DiskBdfEnable {
				if allowed, err := forceDetachAllowed(disk, disk.InstanceId); err != nil {
					errMsg := fmt.Sprintf("DiskID %s force detach allowed,err:%s", diskID, err.Error())
					log.Errorf(log.TypeDisk, log.StatusAttachVolumeFailed, errMsg)
					return status.Error(codes.Aborted, errMsg)
				} else if !allowed {
					errMsg := fmt.Sprintf("DiskID %s is already attach to instance %s.", diskID, disk.InstanceId)
					log.Errorf(log.TypeDisk, log.StatusAttachVolumeFailed, errMsg)
					return status.Error(codes.Aborted, errMsg)
				}
			}
			detachDiskRequest := ecs.CreateDetachDiskRequest()
			detachDiskRequest.DiskId = disk.DiskId
			detachDiskRequest.InstanceId = disk.InstanceId
			response, err := GlobalConfigVar.EcsClient.DetachDisk(detachDiskRequest)
			if err != nil {
				errMsg := fmt.Sprintf("Detach DiskID %s is failed, err:%s", disk.DiskId, err.Error())
				return status.Error(codes.Aborted, errMsg)
			}

			// check disk detach
			for i := 0; i < 25; i++ {
				tmpDisk, err := findDiskByID(diskID)
				if err != nil {
					errMsg := fmt.Sprintf("DiskID %s is not found,err:%s", diskID, err.Error())
					log.Errorf(log.TypeDisk, log.StatusNotFound, errMsg)
					return status.Error(codes.Aborted, errMsg)
				}
				if tmpDisk == nil {
					errMsg := fmt.Sprintf("DiskID %s is not found,err:%s", diskID, err.Error())
					log.Errorf(log.TypeDisk, log.StatusNotFound, errMsg)
					break
				}
				if tmpDisk.InstanceId == "" {
					break
				}
				// Attached by other Instance
				if tmpDisk.InstanceId != nodeID {
					errMsg := fmt.Sprintf("DiskID %s is already attach to instance %s.", diskID, disk.InstanceId)
					log.Errorf(log.TypeDisk, log.StatusAttachVolumeFailed, errMsg)
					break
				}
				// Detach Finish
				if tmpDisk.Status == DiskStatusAvailable {
					break
				}
				// Disk is InUse in same host, but is attached again.
				if tmpDisk.Status == DiskStatusInuse {
					if beforeAttachTime != tmpDisk.AttachedTime {
						log.Infof(log.TypeDisk, log.StatusDetachVolumeFailed, fmt.Sprintf("DiskId %s is attached again, old attachTime: %s, new attachTime: %s", diskID, beforeAttachTime, tmpDisk.AttachedTime))
						break
					}
				}
				if tmpDisk.Status == DiskStatusAttaching {
					log.Infof(log.TypeDisk, log.StatusAttachVolumeFailed, fmt.Sprintf("DiskId %s is attaching to: %s", diskID, tmpDisk.InstanceId))
					break
				}
				if i == 24 {
					errMsg := fmt.Sprintf("Detaching Disk %s with timeout", diskID)
					log.Errorf(log.TypeDisk, log.StatusDetachVolumeFailed, errMsg)
					return status.Error(codes.Aborted, errMsg)
				}
				time.Sleep(2000 * time.Millisecond)
			}
			log.Infof(log.TypeDisk, log.StatusOK, fmt.Sprintf("Volume: %s Success to detach disk %s from Instance %s, RequestId: %s", diskID, disk.DiskId, disk.InstanceId, response.RequestId))
		} else {
			log.Infof(log.TypeDisk, log.StatusDetachVolumeFailed, fmt.Sprintf("Skip Detach for volume: %s, disk %s is attached to other instance: %s", diskID, disk.DiskId, disk.InstanceId))
		}
	} else {
		log.Infof(log.TypeDisk, log.StatusDetachVolumeFailed, fmt.Sprintf("Skip Detach, disk %s have not detachable instance", diskID))
	}

	return nil
}

// tag disk with: k8s.aliyun.com=true
func tagDiskAsK8sAttached(diskID string) {
	// Step 1: Describe disk, if tag exist, return;
	describeDisksRequest := ecs.CreateDescribeDisksRequest()
	describeDisksRequest.RegionId = GlobalConfigVar.Region
	describeDisksRequest.DiskIds = "[\"" + diskID + "\"]"
	diskResponse, err := GlobalConfigVar.EcsClient.DescribeDisks(describeDisksRequest)
	if err != nil {
		log.Errorf(log.TypeDisk, log.StatusEcsApiErr, fmt.Sprintf("Describe disk %s is failed, err:%s", diskID, err.Error()))
		return
	}
	disks := diskResponse.Disks.Disk
	if len(disks) == 0 {
		log.Errorf(log.TypeDisk, log.StatusNotFound, fmt.Sprintf("Disk %s is not found.", diskID))
		return
	}
	for _, tag := range disks[0].Tags.Tag {
		if tag.TagKey == DiskAttachedKey && tag.TagValue == DiskAttachedValue {
			return
		}
	}

	// Step 2: Describe tag
	describeTagRequest := ecs.CreateDescribeTagsRequest()
	tag := ecs.DescribeTagsTag{Key: DiskAttachedKey, Value: DiskAttachedValue}
	describeTagRequest.Tag = &[]ecs.DescribeTagsTag{tag}
	_, err = GlobalConfigVar.EcsClient.DescribeTags(describeTagRequest)
	if err != nil {
		log.Errorf(log.TypeDisk, log.StatusEcsApiErr, fmt.Sprintf("Describe disk %s is failed, err:%s", diskID, err.Error()))
		return
	}

	// Step 3: create & attach tag
	addTagsRequest := ecs.CreateAddTagsRequest()
	tmpTag := ecs.AddTagsTag{Key: DiskAttachedKey, Value: DiskAttachedValue}
	addTagsRequest.Tag = &[]ecs.AddTagsTag{tmpTag}
	addTagsRequest.ResourceType = "disk"
	addTagsRequest.ResourceId = diskID
	addTagsRequest.RegionId = GlobalConfigVar.Region
	_, err = GlobalConfigVar.EcsClient.AddTags(addTagsRequest)
	if err != nil {
		log.Errorf(log.TypeDisk, log.StatusEcsApiErr, fmt.Sprintf("DiskID %s add tag %s is failed, err:%s", diskID, DiskAttachedKey))
		return
	}
	log.Infof(log.TypeDisk, log.StatusOK, fmt.Sprintf("Add tag to disk: %s", diskID))
}

func waitForSharedDiskInStatus(retryCount int, interval time.Duration, diskID, nodeID string, expectStatus string) error {
	for i := 0; i < retryCount; i++ {
		time.Sleep(interval)
		disk, err := findDiskByID(diskID)
		if err != nil {
			return err
		}
		if disk == nil {
			errMsg := fmt.Sprintf("Disk %s is not exist.", diskID)
			log.Errorf(log.TypeDisk, log.StatusNotFound, errMsg)
			return status.Errorf(codes.Aborted, errMsg)
		}
		for _, instance := range disk.MountInstances.MountInstance {
			if expectStatus == DiskStatusAttached {
				if instance.InstanceId == nodeID {
					return nil
				}
			} else if expectStatus == DiskStatusDetached {
				if instance.InstanceId != nodeID {
					return nil
				}
			}
		}
	}
	return status.Errorf(codes.Aborted, fmt.Sprintf("After %d times of check, disk %s is still not attached", retryCount, diskID))
}

func waitForDiskInStatus(retryCount int, interval time.Duration, diskID string, expectedStatus string) error {
	for i := 0; i < retryCount; i++ {
		time.Sleep(interval)
		disk, err := findDiskByID(diskID)
		if err != nil {
			return err
		}
		if disk == nil {
			errMsg := fmt.Sprintf("Disk %s is not exist.", diskID)
			log.Errorf(log.TypeDisk, log.StatusNotFound, errMsg)
			return errors.New(errMsg)
		}
		if disk.Status == expectedStatus {
			return nil
		}
	}
	return status.Errorf(codes.Aborted, fmt.Sprintf("After %d times of check, disk %s is still not in expected status %v", retryCount, diskID, expectedStatus))
}

// return disk with the define name
func findDiskByName(name string, resourceGroupID string, sharedDisk bool) ([]ecs.Disk, error) {
	resDisks := []ecs.Disk{}
	describeDisksRequest := ecs.CreateDescribeDisksRequest()
	describeDisksRequest.RegionId = GlobalConfigVar.Region
	describeDisksRequest.DiskName = name
	diskResponse, err := GlobalConfigVar.EcsClient.DescribeDisks(describeDisksRequest)

	if err != nil {
		return resDisks, err
	}
	if sharedDisk && len(diskResponse.Disks.Disk) == 0 {
		describeDisksRequest.EnableShared = requests.NewBoolean(true)
		diskResponse, err = GlobalConfigVar.EcsClient.DescribeDisks(describeDisksRequest)
		if err != nil {
			return resDisks, err
		}
		if diskResponse == nil {
			errMsg := fmt.Sprintf("Describe disk %s is failed, err:response is empty.", name)
			log.Errorf(log.TypeDisk, log.StatusEcsApiErr, errMsg)
			return nil, status.Errorf(codes.Aborted, errMsg)
		}
	}
	for _, disk := range diskResponse.Disks.Disk {
		if disk.DiskName == name {
			resDisks = append(resDisks, disk)
		}
	}
	return resDisks, err
}

func findDiskByID(diskID string) (*ecs.Disk, error) {
	describeDisksRequest := ecs.CreateDescribeDisksRequest()
	describeDisksRequest.RegionId = GlobalConfigVar.Region
	describeDisksRequest.DiskIds = "[\"" + diskID + "\"]"
	diskResponse, err := GlobalConfigVar.EcsClient.DescribeDisks(describeDisksRequest)
	if err != nil {
		errMsg := fmt.Sprintf("Descirbe disk %s is failed, err:", diskID, err.Error())
		log.Errorf(log.TypeDisk, log.StatusEcsApiErr, errMsg)
		return nil, status.Errorf(codes.Aborted, errMsg)
	}
	disks := diskResponse.Disks.Disk
	// shared disk can not described if not set EnableShared
	if len(disks) == 0 {
		describeDisksRequest.EnableShared = requests.NewBoolean(true)
		diskResponse, err = GlobalConfigVar.EcsClient.DescribeDisks(describeDisksRequest)
		if err != nil {
			if strings.Contains(err.Error(), UserNotInTheWhiteList) {
				return nil, nil
			}
			errMsg := fmt.Sprintf("Disk %s is not exist.", diskID)
			log.Errorf(log.TypeDisk, log.StatusNotFound, errMsg)
			return nil, status.Errorf(codes.Aborted, errMsg)
		}
		if diskResponse == nil {
			errMsg := fmt.Sprintf("Describe disk %s is failed, err:response is empty.", diskID)
			log.Errorf(log.TypeDisk, log.StatusEcsApiErr, errMsg)
			return nil, status.Errorf(codes.Aborted, errMsg)
		}
		disks = diskResponse.Disks.Disk
	}

	if len(disks) == 0 {
		return nil, nil
	}
	if len(disks) > 1 {
		errMsg := fmt.Sprintf("Unexpected count %d for volume id %s, Get Response: %v, with Request: %v, %v", len(disks), diskID, diskResponse, describeDisksRequest.RegionId, describeDisksRequest.DiskIds)
		log.Errorf(log.TypeDisk, log.StatusEcsApiErr, errMsg)
		return nil, status.Errorf(codes.Internal, errMsg)
	}
	return &disks[0], err
}

func findSnapshotByName(name string) (*diskSnapshot, int, error) {
	describeSnapShotRequest := ecs.CreateDescribeSnapshotsRequest()
	describeSnapShotRequest.RegionId = GlobalConfigVar.Region
	describeSnapShotRequest.SnapshotName = name
	snapshots, err := GlobalConfigVar.EcsClient.DescribeSnapshots(describeSnapShotRequest)
	if err != nil {
		return nil, 0, err
	}
	if len(snapshots.Snapshots.Snapshot) == 0 {
		return nil, 0, nil
	}
	existSnapshot := snapshots.Snapshots.Snapshot[0]
	t, err := time.Parse(time.RFC3339, existSnapshot.CreationTime)
	if err != nil {
		errMsg := fmt.Sprintf("Failed to parse snapshot creation time: %s", existSnapshot.CreationTime)
		log.Errorf(log.TypeDisk, log.StatusInternalError, errMsg)
		return nil, 0, status.Errorf(codes.Internal, errMsg)
	}
	timestamp := timestamp.Timestamp{Seconds: t.Unix()}
	sizeGb, _ := strconv.ParseInt(existSnapshot.SourceDiskSize, 10, 64)
	sizeBytes := sizeGb * 1024 * 1024
	readyToUse := false
	if existSnapshot.Status == "accomplished" {
		readyToUse = true
	}

	resSnapshot := &diskSnapshot{
		Name:         name,
		ID:           existSnapshot.SnapshotId,
		VolID:        existSnapshot.SourceDiskId,
		CreationTime: timestamp,
		SizeBytes:    sizeBytes,
		ReadyToUse:   readyToUse,
	}
	if len(snapshots.Snapshots.Snapshot) > 1 {
		errMsg := fmt.Sprintf("Find more than one snapshot with name %s", name)
		log.Errorf(log.TypeDisk, log.StatusInternalError, errMsg)
		return resSnapshot, len(snapshots.Snapshots.Snapshot), status.Errorf(codes.Internal, errMsg)
	}
	return resSnapshot, 1, nil
}

func findDiskSnapshotByID(id string) (*diskSnapshot, int, error) {
	describeSnapShotRequest := ecs.CreateDescribeSnapshotsRequest()
	describeSnapShotRequest.RegionId = GlobalConfigVar.Region
	describeSnapShotRequest.SnapshotIds = "[\"" + id + "\"]"
	snapshots, err := GlobalConfigVar.EcsClient.DescribeSnapshots(describeSnapShotRequest)
	if err != nil {
		return nil, 0, err
	}
	if len(snapshots.Snapshots.Snapshot) == 0 {
		return nil, 0, nil
	}

	existSnapshot := snapshots.Snapshots.Snapshot[0]
	t, err := time.Parse(time.RFC3339, existSnapshot.CreationTime)
	if err != nil {
		errMsg := fmt.Sprintf("Failed to parse snapshot creation time: %s", existSnapshot.CreationTime)
		log.Errorf(log.TypeDisk, log.StatusInternalError, errMsg)
		return nil, 0, errors.New(errMsg)
	}
	timestamp := timestamp.Timestamp{Seconds: t.Unix()}
	sizeGb, _ := strconv.ParseInt(existSnapshot.SourceDiskSize, 10, 64)
	sizeBytes := sizeGb * 1024 * 1024
	readyToUse := false
	if existSnapshot.Status == "accomplished" {
		readyToUse = true
	}

	resSnapshot := &diskSnapshot{
		Name:         id,
		ID:           existSnapshot.SnapshotId,
		VolID:        existSnapshot.SourceDiskId,
		CreationTime: timestamp,
		SizeBytes:    sizeBytes,
		ReadyToUse:   readyToUse,
		SnapshotTags: existSnapshot.Tags.Tag,
	}
	if len(snapshots.Snapshots.Snapshot) > 1 {
		errMsg := fmt.Sprintf("Find more than one snapshot with name %s", id)
		log.Errorf(log.TypeDisk, log.StatusInternalError, errMsg)
		return resSnapshot, len(snapshots.Snapshots.Snapshot), status.Error(codes.Internal, errMsg)
	}
	return resSnapshot, 1, nil
}
