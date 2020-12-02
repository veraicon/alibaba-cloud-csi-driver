package agent

import (
	"encoding/json"
	"fmt"
	"github.com/golang/glog"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	. "github.com/kubernetes-sigs/alibaba-cloud-csi-driver/pkg/logs"
	"github.com/kubernetes-sigs/alibaba-cloud-csi-driver/pkg/utils"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	// queryServerSocket tag, used for queryserver socket
	queryServerSocket = "/var/run/node-extender-server/volume-query-server.sock"
)

// QueryRequest struct
// Identity: for volumeInfo Request
// PodName/PodNameSpace: PodRunTime Request
type QueryRequest struct {
	Identity     string `json:"identity"`
	PodName      string `json:"podName"`
	PodNameSpace string `json:"podNameSpace"`
}

// QueryServer Kata Server
type QueryServer struct {
	client kubernetes.Interface
}

// NewQueryServer new server
func NewQueryServer() *QueryServer {
	cfg, err := clientcmd.BuildConfigFromFlags("", "")
	if err != nil {
		glog.Fatal(GetLogInfoByErrorCode(StatusGetKubeConfigFailed, cfg.String(), err.Error()))
	}

	kubeClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		glog.Fatal(GetLogInfoByErrorCode(StatusGetKubeConfigFailed, cfg.String(), err.Error()))
	}
	return &QueryServer{
		client: kubeClient,
	}
}

// RunQueryServer Routers
func (ks *QueryServer) RunQueryServer() {
	socketAddr := &net.UnixAddr{Name: queryServerSocket, Net: "unix"}
	os.Remove(socketAddr.Name)
	lis, err := net.ListenUnix("unix", socketAddr)
	if err != nil {
		glog.Fatal(GetLogInfoByErrorCode(StatusSocketListenFailed, queryServerSocket, err.Error()))
		return
	}

	// set router
	glog.Infof("Started Query Server with unix socket: %s", queryServerSocket)
	http.HandleFunc("/api/v1/volumeinfo", ks.volumeInfoHandler)
	//	http.HandleFunc("/api/v1/podruntime", ks.podRunTimeHander)
	http.HandleFunc("/api/v1/ping", ks.pingHandler)

	// Server Listen
	svr := &http.Server{Handler: http.DefaultServeMux}
	err = svr.Serve(lis)
	if err != nil {
		glog.Errorf(GetLogInfoByErrorCode(StatusSocketListenFailed, queryServerSocket, err.Error()))
	}
	glog.Infof("Query Server Ending ....")
}

// volumeInfoHandler reply with volume options.
func (ks *QueryServer) volumeInfoHandler(w http.ResponseWriter, r *http.Request) {
	reqInfo := QueryRequest{}
	content, err := ioutil.ReadAll(r.Body)
	if err != nil {
		glog.Error(StatusSocketReadBufferFailed, r.Host, err.Error())
		fmt.Fprintf(w, "null")
		return
	}
	if err := json.Unmarshal(content, &reqInfo); err != nil {
		glog.Error(StatusParseJsonFailed, content, err.Error())
		fmt.Fprintf(w, "null")
		return
	}

	if reqInfo.Identity == "" {
		fmt.Fprintf(w, "null")
		return
	}

	// Response with file content
	fileName := filepath.Join(reqInfo.Identity, utils.CsiPluginRunTimeFlagFile)
	if utils.IsFileExisting(fileName) {
		// Unmarshal file content to map
		fileContent := utils.GetFileContent(fileName)
		fileContent = strings.ToLower(fileContent)
		volInfoMapFrom := map[string]string{}
		if err := json.Unmarshal([]byte(fileContent), &volInfoMapFrom); err != nil {
			glog.Error(StatusParseJsonFailed, fileContent, err.Error())
			fmt.Fprintf(w, "null")
			return
		}
		volumeType := ""
		if value, ok := volInfoMapFrom["volumetype"]; ok {
			volumeType = value
		}
		// copy parts of items to new map
		volInfoMapResponse := map[string]string{}
		// for disk volume type
		if volumeType == "block" {
			if value, ok := volInfoMapFrom["device"]; ok {
				volInfoMapResponse["path"] = value
			}
			if value, ok := volInfoMapFrom["identity"]; ok {
				volInfoMapResponse["identity"] = value
			}
			volInfoMapResponse["volumeType"] = "block"
			// for nas volume type
		} else if volumeType == "nfs" {
			if value, ok := volInfoMapFrom["server"]; ok {
				volInfoMapResponse["server"] = value
			}
			if value, ok := volInfoMapFrom["path"]; ok {
				volInfoMapResponse["path"] = value
			}
			if value, ok := volInfoMapFrom["vers"]; ok {
				volInfoMapResponse["vers"] = value
			} else {
				volInfoMapResponse["vers"] = "3"
			}
			if value, ok := volInfoMapFrom["mode"]; ok {
				volInfoMapResponse["mode"] = value
			} else {
				volInfoMapResponse["mode"] = ""
			}
			if value, ok := volInfoMapFrom["options"]; ok {
				volInfoMapResponse["options"] = value
			} else {
				volInfoMapResponse["options"] = "noresvport,nolock,tcp"
			}
			volInfoMapResponse["volumeType"] = "nfs"
		} else {
			glog.Error(StatusVolumeTypeUnknown,  reqInfo.Identity, volumeType)
			fmt.Fprintf(w, "null")
			return
		}

		responseStr, err := json.Marshal(volInfoMapResponse)
		if err != nil {
			glog.Error(StatusParseJsonFailed,  volInfoMapResponse, err.Error())
			fmt.Fprintf(w, "null")
			return
		}

		// Send response
		fmt.Fprintf(w, string(responseStr))
		glog.Infof("Request volumeInfo: Send Successful Response with: %s", responseStr)
		return
	}

	// no found volume
	glog.Warning(StatusVolumeNotFound, fileName)
	fmt.Fprintf(w, "no found volume: %s", fileName)
	return

}

// pingHandler ping test
func (ks *QueryServer) pingHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Ping successful")
}
