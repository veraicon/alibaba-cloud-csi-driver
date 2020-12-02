package logs

//program ErrorCode
const (
	StatusArgsInvalid = iota
	StatusAttachDiskProcessing
	StatusBdfTagClearFailed
	StatusBdfTagNotFound
	StatusBdfTagStoreFailed
	StatusCreateMountPathFailed
	StatusCreateVolumeFailed
	StatusDeleteVolumeFailed
	StatusDiskAttachFailed
	StatusDiskAttachOtherInstance
	StatusDiskDescribeTagFailed
	StatusDiskDetachFailed
	StatusDiskDetachTimeout
	StatusDiskNotFound
	StatusDiskRepeatAttach
	StatusDiskWaitDetach
	StatusEcsAddTagFailed
	StatusEcsDescribeDiskFailed
	StatusEcsDiskNotPortable
	StatusEcsDiskIDEmpty
	StatusEcsInstanceDiskLimitExceeded
	StatusEcsNotSupportDiskCategory
	StatusEcsRemoveTagFailed
	StatusErrorCodeNotFound
	StatusExecuteCommandFailed
	StatusFileNotExist
	StatusFileRenameFailed
	StatusGetKubeConfigFailed
	StatusGetMetricsFailed
	StatusGetPvFailed
	StatusMountFailed
	StatusMountPointExist
	StatusOptionInvalid
	StatusParseJsonFailed
	StatusPciBindFailed
	StatusPciUnbindFailed
	StatusPvFileSystemNotFound
	StatusPvServerNotFound
	StatusPvSpecNotCSI
	StatusPvStorageClassNotFound
	StatusPvSubPathNotFound
	StatusReadlinkFailed
	StatusEcsResourcesNotInSameZone
	StatusSocketAcceptFailed
	StatusSocketConnectionFailed
	StatusSocketFindManyDead
	StatusSocketListenFailed
	StatusSocketReadBufferFailed
	StatusSocketWriteBufferFailed
	StatusStorageClassNotFound
	StatusUnknown
	StatusVolumeNotFound
	StatusVolumeTypeUnknown
)

var errorCodeMap = map[int]logInfo{
	StatusAttachDiskProcessing:         {reason: "AttachDiskProcessing", message: "Previous volumeid %s attach action is still in process"},
	StatusBdfTagClearFailed:            {reason: "BdfTagClearFailed", message: "DiskID %s's clear bdf tags %s is failed, err:%s"},
	StatusBdfTagNotFound:               {reason: "BdfTagNotFound", message: "DiskID %s's bdf tags is not found, err:%s"},
	StatusBdfTagStoreFailed:            {reason: "BdfTagStoreFailed", message: "DiskID %s's bdf tags store is failed, err:%s"},
	StatusCreateMountPathFailed:        {reason: "CreateMountPathFailed", message: "Create mount path %s is failed, err:%s"},
	StatusCreateVolumeFailed:           {reason: "CreateVolumeFailed", message: "Create Volume %s is failed, err:%s."},
	StatusDeleteVolumeFailed:           {reason: "DeleteVolumeFailed", message: "Controller delete volume %s is failed, err:%s"},
	StatusDiskAttachFailed:             {reason: "DiskAttachFailed", message: "Attach disk %s to instance %s is failed, err:%s"},
	StatusDiskAttachOtherInstance:      {reason: "DiskAttachOtherInstance", message: "DiskID %s is attached by other instance %s, not as before %s"},
	StatusDiskDescribeTagFailed:        {reason: "DiskDescribeTagFailed", message: "DiskID %s describe tags is failed, err:%s"},
	StatusDiskDetachFailed:             {reason: "DiskDetachFailed", message: "DiskID %s is detach failed, err:%s"},
	StatusDiskDetachTimeout:            {reason: "DiskDetachTimeout", message: "DiskID %s detach is timeout."},
	StatusDiskNotFound:                 {reason: "DiskNotFound", message: "DiskID %s is not found."},
	StatusDiskRepeatAttach:             {reason: "DiskRepeatAttach", message: "DiskID %s is already attached to Instance %s"},
	StatusDiskWaitDetach:               {reason: "DiskWaitDetach", message: "Wait for DiskID %s is detached."},
	StatusEcsAddTagFailed:              {reason: "EcsAddTagFailed", message: "Ecs's DiskID %s add tag is failed, err:%s"},
	StatusEcsDescribeDiskFailed:        {reason: "EcsDescribeDiskFailed", message: "Ecs's DiskID %s describe disk is failed, err:%s"},
	StatusEcsDiskIDEmpty:               {reason: "EcsDiskIDEmpty", message: "DiskID %s's id is empty."},
	StatusEcsDiskNotPortable:           {reason: "EcsDiskNotPortable.", message: "The specified disk %s is not a portable disk.", recommend: "Please see https://error-center.aliyun.com/status/search?Keyword=DiskNotPortable&source=PopGw."},
	StatusEcsResourcesNotInSameZone:    {reason: "EcsResourcesNotInSameZone.", message: "The specified instance and disk are not in the same zone.The disk %s is in %s, the node %s is in %s.", recommend: "Please see https://error-center.aliyun.com/status/search?Keyword=ResourcesNotInSameZone&source=PopGw."},
	StatusEcsInstanceDiskLimitExceeded: {reason: "EcsInstanceDiskLimitExceeded.", message: "The amount of the disk on instance in question reach its limits. Node %s exceed the limit attachments of disk, which at most 16 disks.", recommend: "Please see https://error-center.aliyun.com/status/search?Keyword=InstanceDiskLimitExceeded&source=PopGw."},
	StatusEcsNotSupportDiskCategory:    {reason: "EcsNotSupportDiskCategory.", message: "The instanceType %s is not support diskType %s mount.", recommend: "The current Node %s is %s instanceType, only supports %s diskType."},
	StatusEcsRemoveTagFailed:           {reason: "EcsRemoveTagFailed", message: "Ecs's DiskID %s remove tag is failed, err:%s"},
	StatusErrorCodeNotFound:            {reason: "ErrorCodeNotFound.", message: "Error code %s is not found.", recommend: "Please define error code、reason、message and recommend."},
	StatusExecuteCommandFailed:         {reason: "ExecuteCommandFailed", message: "Execute command:%s is failed, err:%s"},
	StatusFileNotExist:                 {reason: "FileNotExist", message: "File %s is not exist."},
	StatusFileRenameFailed:             {reason: "FileRenameFailed", message: "File %s rename to %s is failed, err:%s"},
	StatusGetKubeConfigFailed:          {reason: "StatusGetKubeConfigFailed", message: "Get kubeconfig %s is failed, err:%s"},
	StatusGetMetricsFailed:             {reason: "GetMetricsFailed.", message: "Failed to fetch %s metrics."},
	StatusGetPvFailed:                  {reason: "GetPvFailed", message: "Get PV %s is failed, err:%s"},
	StatusMountFailed:                  {reason: "MountFailed", message: "Volume %s is mount failed, err:%s"},
	StatusMountPointExist:              {reason: "MountPointExist", message: "Mount point %s is exist."},
	StatusOptionInvalid:                {reason: "OptionInvalid.", message: "%s mount option is invalid."},
	StatusParseJsonFailed:              {reason: "ParseJsonFailed", message: "Parse json %s path is failed, err:%s"},
	StatusPciBindFailed:                {reason: "PciBindFailed", message: "DiskID %s's bdf tags %s bind is failed, err:%s"},
	StatusPciUnbindFailed:              {reason: "VirtioPciUnbind", message: "DiskID %s's bdf tags %s unbind is failed, err:%s"},
	StatusPvFileSystemNotFound:         {reason: "PvFileSystemNotFound", message: "Pv volume %s's filesystem is not found."},
	StatusPvServerNotFound:             {reason: "ServerNotFound", message: "Pv volume %s's server is not found."},
	StatusPvSpecNotCSI:                 {reason: "PvSpecNotCSI", message: "Pv volume %s is not csi type."},
	StatusPvStorageClassNotFound:       {reason: "PvStorageClassNotFound", message: "Pv volume %s's storageclass is not found."},
	StatusPvSubPathNotFound:            {reason: "PvSubPathNotFound", message: "Pv volume %s's subpath is not found."},
	StatusReadlinkFailed:               {reason: "ReadlinkFailed", message: "DiskID %s's bdf tags %s readlink is failed, err:%s"},
	StatusSocketAcceptFailed:           {reason: "SocketAcceptFailed", message: "Socket server %s accept is failed, err: %s."},
	StatusSocketConnectionFailed:       {reason: "SocketConnectionFailed.", message: "Connector %s is not running, err:%s."},
	StatusSocketFindManyDead:           {reason: "SocketFindManyDead", message: "Watchdog find too many dead sockets, %s will exit(0)."},
	StatusSocketListenFailed:           {reason: "SocketListenFailed.", message: "Socket server path %s listen is failed, err: %s."},
	StatusSocketReadBufferFailed:       {reason: "SocketReadBufferFailed", message: "Socket server %s read buffer, err: %s."},
	StatusSocketWriteBufferFailed:      {reason: "SocketWriteBufferFailed", message: "Socket server %s write buffer, err:%s."},
	StatusStorageClassNotFound:         {reason: "StorageClassNotFound", message: "StorgeClass %s is not found."},
	StatusUnknown:                      {reason: "Unknown.", message: "%s status is Unknown."},
	StatusVolumeNotFound:               {reason: "VolumeNotFound", message: "Volume %s is not found."},
	StatusVolumeTypeUnknown:            {reason: "VolumeTypeUnknown", message: "Volume %s's type is unknown, type:%s"},
	StatusArgsInvalid:                  {reason: "LogArgsInvalid.", message: "Args quantity is invalid. Need args number is %s, input args number is %s."},
}
