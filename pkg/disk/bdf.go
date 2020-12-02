package disk

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/ecs"
	"github.com/kubernetes-sigs/alibaba-cloud-csi-driver/pkg/log"
	"github.com/pkg/errors"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
)

const (
	// VfBar0Sz value
	VfBar0Sz = 0x1000
	// DevIDOffsetInBar0 value
	DevIDOffsetInBar0 = 0x100
	// MaxVfNum value
	MaxVfNum = 256
	// BlkIDSz value
	BlkIDSz = 20

	sysPrefix        = "/host"
	iohubSriovAction = sysPrefix + "/sys/bus/pci/drivers/iohub_sriov/"
	virtioPciAction  = sysPrefix + "/sys/bus/pci/drivers/virtio-pci/"

	iohubSrviovDriver = "iohub_sriov"
	virtioPciDriver   = "virtio-pci"

	// InstanceStatusStopped ecs stopped status
	InstanceStatusStopped = "Stopped"
	// DiskBdfTagKey disk bdf tag
	DiskBdfTagKey = "bdf.csi.aliyun.com"
)

// PatchStringValue type
type PatchStringValue struct {
	Op    string      `json:"op"`
	Path  string      `json:"path"`
	Value interface{} `json:"value"`
}

// BdfAttachInfo type
type BdfAttachInfo struct {
	Depend             bool   `json:"depend"`
	LastAttachedNodeID string `json:"last_attached_node_id"`
}

// FindLines parse lines
func FindLines(reader io.Reader, keyword string) []string {
	var matched []string
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, keyword) {
			matched = append(matched, line)
		}
	}
	return matched
}

// IsNoSuchDeviceErr nd device error
func IsNoSuchDeviceErr(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "no such device")
}

// IohubSriovBind io hub bind
func IohubSriovBind(bdf string) error {
	return ioutil.WriteFile(iohubSriovAction+"bind", []byte(bdf), 0600)
}

// IohubSriovUnbind io hub unbind
func IohubSriovUnbind(bdf string) error {
	return ioutil.WriteFile(iohubSriovAction+"unbind", []byte(bdf), 0600)
}

// VirtioPciBind pci bind
func VirtioPciBind(bdf string) error {
	return ioutil.WriteFile(virtioPciAction+"bind", []byte(bdf), 0600)
}

// VirtioPciUnbind pci unbind
func VirtioPciUnbind(bdf string) error {
	return ioutil.WriteFile(virtioPciAction+"unbind", []byte(bdf), 0600)
}

// ExecCheckOutput check output
func ExecCheckOutput(cmd string, args ...string) (io.Reader, error) {
	c := exec.Command(cmd, args...)
	stdout := bytes.NewBuffer(nil)
	stderr := bytes.NewBuffer(nil)
	c.Stdout = stdout
	c.Stderr = stderr
	if err := c.Run(); err != nil {
		return nil, errors.Errorf("cmd: %s, stdout: %s, stderr: %s, err: %v",
			cmd, stdout, stderr, err)
	}

	return stdout, nil
}

func findBdf(diskID string) (bdf string, err error) {
	var (
		domain   int
		bus      int
		dev      int
		function int
		bar0     uint64

		blkIds      = make([]byte, BlkIDSz)
		blkIDSuffix string
	)

	if _, err := fmt.Sscanf(diskID, "d-%s", &blkIDSuffix); err != nil {
		return "", err
	}
	copy(blkIds, blkIDSuffix)

	output, err := ExecCheckOutput("lspci", "-D")
	if err != nil {
		return "", err
	}
	// 0000:a1:00.0 SCSI storage controller: Red Hat, Inc Virtio block device
	matched := FindLines(output, "Virtio block device")
	if len(matched) == 0 {
		return "", nil
	}

	if _, err = fmt.Sscanf(matched[0], "%s SCSI storage controller", &bdf); err != nil {
		return "", err
	}
	if _, err = fmt.Sscanf(bdf, "%04x:%02x:%02x.%d", &domain, &bus, &dev, &function); err != nil {
		return "", errors.Wrapf(err, "bdf format")
	}

	// 找bar0地址
	bdf = fmt.Sprintf("%02x:%02x.%d", bus+1, dev, function)
	output, err = ExecCheckOutput("lspci", "-s", bdf, "-vvvv")
	if err != nil {
		return "", err
	}
	//    Region 0: Memory at 00000000ed501000 (64-bit, prefetchable)
	matched = FindLines(output, "\t\tRegion 0: Memory at")
	// 非bdf机型是匹配不到这个信息的
	if len(matched) == 0 {
		return "", nil
	}
	if _, err = fmt.Sscanf(matched[len(matched)-1], "\t\tRegion 0: Memory at %x", &bar0); err != nil {
		return "", err
	}

	pgsize := syscall.Getpagesize()
	base := bar0 & (^(uint64(pgsize - 1)))
	offset := int(base & uint64(pgsize-1))

	fd, err := os.Open("/dev/mem")
	if err != nil {
		return "", err
	}
	defer fd.Close()
	mmdata, err := syscall.Mmap(int(fd.Fd()), int64(base), MBSIZE, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return "", errors.Wrapf(err, "Mmap")
	}
	defer syscall.Munmap(mmdata)

	bdf = ""
	for i := 0; i < MaxVfNum; i++ {
		pos := offset + i*VfBar0Sz + DevIDOffsetInBar0
		devIds := make([]byte, BlkIDSz)
		for i := 0; i < 5; i++ {
			start := 4 * i
			copy(devIds[start:start+4], mmdata[pos+start:pos+start+4])
		}
		if bytes.Equal(devIds, blkIds) {
			bdf = fmt.Sprintf("%04x:%02x:%02x.%d", domain, bus+1, dev+(i+1)/8, function+(i+1)%8)
			break
		}
	}

	return bdf, nil
}

func unbindBdfDisk(diskID string) (err error) {
	bdf, err := findBdf(diskID)
	if err != nil {
		errMsg := fmt.Sprintf("DiskID %s bdf tag is not found,err:%s", diskID, err.Error())
		log.Errorf(log.TypeDisk, log.StatusNotFound, errMsg)
		return errors.New(errMsg)
	}
	if bdf == "" {
		errMsg := fmt.Sprintf("DiskID %s bdf tag is not found,err:%s", diskID, err.Error())
		log.Errorf(log.TypeDisk, log.StatusNotFound, errMsg)
		return nil
	}

	log.Infof(log.TypeDisk, log.StatusOK, fmt.Sprintf("unbindBdfDisk: Disk %s bdf is %s", diskID, bdf))

	if err := VirtioPciUnbind(bdf); err != nil && !IsNoSuchDeviceErr(err) {
		errMsg := fmt.Sprintf("DiskID %s pci bdf tag %s unbind failed, err:%s", diskID, bdf, err.Error())
		log.Errorf(log.TypeDisk, log.StatusInternalError, errMsg)
		return errors.New(errMsg)
	}

	if err := IohubSriovBind(bdf); err != nil && !IsNoSuchDeviceErr(err) {
		errMsg := fmt.Sprintf("DiskID %s pci bdf tag %s bind failed, err:%s", diskID, bdf, err.Error())
		log.Errorf(log.TypeDisk, log.StatusInternalError, errMsg)
		return errors.New(errMsg)
	}

	if err = clearBdfInfo(diskID, bdf); err != nil {
		errMsg := fmt.Sprintf("DiskID %s pci bdf tag %s delete tag is failed, err:%s", diskID, bdf, err.Error())
		log.Errorf(log.TypeDisk, log.StatusEcsApiErr, errMsg)
		return errors.New(errMsg)
	}
	return nil
}

func bindBdfDisk(diskID string) (bdf string, err error) {
	bdf, err = findBdf(diskID)
	if err != nil {
		errMsg := fmt.Sprintf("DiskID %s bdf tag %s is not found, err:%s", diskID, bdf, err.Error())
		log.Errorf(log.TypeDisk, log.StatusNotFound, errMsg)
		return "", errors.New(errMsg)
	}
	if bdf == "" {
		errMsg := fmt.Sprintf("DiskID %s bdf tag is not found")
		log.Errorf(log.TypeDisk, log.StatusNotFound, errMsg)
		return "", errors.New(errMsg)
	}

	data, err := os.Readlink(sysPrefix + "/sys/bus/pci/devices/" + bdf + "/driver")
	if err != nil {
		errMsg := fmt.Sprintf("DiskID %s bdf tag %s read link is failed, err:%s", diskID, bdf, err.Error())
		log.Errorf(log.TypeDisk, log.StatusInternalError, errMsg)
		return bdf, errors.New(errMsg)
	}
	driver := filepath.Base(data)

	switch driver {
	case iohubSrviovDriver:
		if err = IohubSriovUnbind(bdf); err != nil {
			errMsg := fmt.Sprintf("DiskID %s bdf tag %s unbind is failed, err:%s", diskID, bdf, err.Error())
			log.Errorf(log.TypeDisk, log.StatusUmountFailed, errMsg)
			return bdf, errors.New(errMsg)
		}
	case virtioPciDriver:
		if err = storeBdfInfo(diskID, bdf); err != nil {
			errMsg := fmt.Sprintf("DiskID %s bdf tag %s is store is failed, err:%s", diskID, bdf, err.Error())
			log.Errorf(log.TypeDisk, log.StatusEcsApiErr, errMsg)
			return bdf, errors.New(errMsg)
		}
		return bdf, nil
	}
	if err = VirtioPciBind(bdf); err != nil && !IsNoSuchDeviceErr(err) {
		errMsg := fmt.Sprintf("DiskID %s bdf tag %s bind is failed, err:%s", diskID, bdf, err.Error())
		log.Errorf(log.TypeDisk, log.StatusMountFailed, errMsg)
		return bdf, errors.New(errMsg)
	}
	log.Infof(log.TypeDisk, log.StatusOK, fmt.Sprintf("bindBdfDisk: Disk %s(%s) successfully", diskID, bdf))

	if err = storeBdfInfo(diskID, bdf); err != nil {
		errMsg := fmt.Sprintf("DiskID %s bdf tag %s is store is failed, err:%s", diskID, bdf, err.Error())
		log.Errorf(log.TypeDisk, log.StatusEcsApiErr, errMsg)
		return bdf, errors.New(errMsg)
	}
	return bdf, nil
}

func storeBdfInfo(diskID, bdf string) (err error) {
	info := BdfAttachInfo{
		Depend:             bdf != "",
		LastAttachedNodeID: GlobalConfigVar.NodeID,
	}
	infoBytes, _ := json.Marshal(info)

	// Step 2: create & attach tag
	addTagsRequest := ecs.CreateAddTagsRequest()
	tmpTag := ecs.AddTagsTag{Key: DiskBdfTagKey, Value: string(infoBytes)}
	addTagsRequest.Tag = &[]ecs.AddTagsTag{tmpTag}
	addTagsRequest.ResourceType = "disk"
	addTagsRequest.ResourceId = diskID
	addTagsRequest.RegionId = GlobalConfigVar.Region
	GlobalConfigVar.EcsClient = updateEcsClent(GlobalConfigVar.EcsClient)
	_, err = GlobalConfigVar.EcsClient.AddTags(addTagsRequest)
	if err != nil {
		errMsg := fmt.Sprintf("DiskID %s add tag by ecs is failed, err:%s", diskID, err.Error())
		log.Errorf(log.TypeDisk, log.StatusEcsApiErr, errMsg)
		return
	}
	return nil
}

func clearBdfInfo(diskID, bdf string) (err error) {
	info := BdfAttachInfo{
		Depend:             bdf != "",
		LastAttachedNodeID: GlobalConfigVar.NodeID,
	}
	infoBytes, _ := json.Marshal(info)

	removeTagsRequest := ecs.CreateRemoveTagsRequest()
	tmpTag := ecs.RemoveTagsTag{Key: DiskBdfTagKey, Value: string(infoBytes)}
	removeTagsRequest.Tag = &[]ecs.RemoveTagsTag{tmpTag}
	removeTagsRequest.ResourceType = "disk"
	removeTagsRequest.ResourceId = diskID
	removeTagsRequest.RegionId = GlobalConfigVar.Region
	GlobalConfigVar.EcsClient = updateEcsClent(GlobalConfigVar.EcsClient)
	_, err = GlobalConfigVar.EcsClient.RemoveTags(removeTagsRequest)
	if err != nil {
		errMsg := fmt.Sprintf("DiskID %s add tag by ecs is failed, err:%s", diskID, err.Error())
		log.Errorf(log.TypeDisk, log.StatusEcsApiErr, errMsg)
		return
	}

	return nil
}

func forceDetachAllowed(disk *ecs.Disk, nodeID string) (allowed bool, err error) {
	// The following case allow detach:
	// 1. no depend bdf
	// 2. instance status is stopped

	// case 1
	describeDisksRequest := ecs.CreateDescribeDisksRequest()
	describeDisksRequest.RegionId = GlobalConfigVar.Region
	describeDisksRequest.DiskIds = "[\"" + disk.DiskId + "\"]"
	diskResponse, err := GlobalConfigVar.EcsClient.DescribeDisks(describeDisksRequest)
	if err != nil {
		errMsg := fmt.Sprintf("DiskID %s describe disk by ecs %s is failed,err:%s", disk.DiskId, disk.InstanceId, err.Error())
		log.Errorf(log.TypeDisk, log.StatusEcsApiErr, errMsg)
		return false, errors.New(errMsg)
	}
	disks := diskResponse.Disks.Disk
	if len(disks) == 0 {
		errMsg := fmt.Sprintf("DiskID %s reqeust ecs return disks is empty.", disk.DiskId)
		log.Errorf(log.TypeDisk, log.StatusEcsApiErr, errMsg)
		return false, errors.New(errMsg)
	}
	bdfTagExist := false
	for _, tag := range disks[0].Tags.Tag {
		if tag.TagKey == DiskBdfTagKey {
			bdfTagExist = true
		}
	}
	if !bdfTagExist {
		return true, nil
	}

	request := ecs.CreateDescribeInstancesRequest()
	request.RegionId = disk.RegionId
	request.InstanceIds = "[\"" + disk.InstanceId + "\"]"
	instanceResponse, err := GlobalConfigVar.EcsClient.DescribeInstances(request)
	if err != nil {
		return false, errors.Wrapf(err, "DescribeInstances, instanceId=%s", disk.InstanceId)
	}
	if len(instanceResponse.Instances.Instance) == 0 {
		return false, errors.Errorf("Describe Instance with empty response: %s", disk.InstanceId)
	}
	inst := instanceResponse.Instances.Instance[0]
	// case 2
	return inst.Status == InstanceStatusStopped, nil
}

var vfOnce = new(sync.Once)
var isVF = false

// IsVFNode returns whether the current node is vf
func IsVFNode() bool {
	vfOnce.Do(func() {
		output, err := ExecCheckOutput("lspci", "-D")
		if err != nil {
			errMsg := fmt.Sprintf("Execute command  lspci -D is failed, err:%s", err.Error())
			log.Errorf(log.TypeDisk, log.StatusExecuCmdFailed, errMsg)
		}
		// 0000:4b:00.0 SCSI storage controller: Device 1ded:1001
		matched := FindLines(output, "storage controller")
		if len(matched) == 0 {
			log.Errorf(log.TypeDisk, log.StatusInternalError, "[IsVFNode] not found storage controller")
			return
		}
		for _, line := range matched {
			// 1ded: is alibaba cloud
			if !strings.Contains(line, "1ded:") {
				continue
			}
			bdf := strings.SplitN(line, " ", 2)[0]
			if !strings.HasSuffix(bdf, ".0") {
				continue
			}
			output, err = ExecCheckOutput("lspci", "-s", bdf, "-v")
			if err != nil {
				errMsg := fmt.Sprintf("Execute command lspic -s %s -v is faield, err:%s", bdf, err.Error())
				log.Errorf(log.TypeDisk, log.StatusExecuCmdFailed, errMsg)
				return
			}
			// Capabilities: [110] Single Root I/O Virtualization (SR-IOV)
			matched = FindLines(output, "Single Root I/O Virtualization")
			if len(matched) > 0 {
				isVF = true
				break
			}
		}
	})
	return isVF
}
