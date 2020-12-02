package logs

import (
	"fmt"
	"strconv"
	"strings"
)

type logInfo struct {
	reason     string
	message    string
	recommend  string
}

func getLogInfoByErrorCode(code int) string {
	logInfo := errorCodeMap[code]
	logStr := "reason:" + logInfo.reason + "message:" + logInfo.message + "recommend:" + logInfo.recommend
	return logStr
}

func GetLogInfoByErrorCode(code int, args... string) string {
	//errorCode is not found
	var logStr string
	if _, ok := errorCodeMap[code]; !ok {
		logStr = getLogInfoByErrorCode(StatusErrorCodeNotFound)
		logStr =  fmt.Sprintf(logStr, strconv.Itoa(code))
		return logStr
	}

	//len(args) != logInfo.argsAmount
	logStr = getLogInfoByErrorCode(code)
	paddingCount := strings.Count(logStr, "%s")
	if len(args) != paddingCount {
		logStr = getLogInfoByErrorCode(StatusArgsInvalid)
		logStr = fmt.Sprintf(logStr, paddingCount, strconv.Itoa(len(args)))
		return logStr
	}

	interfaceArray := make([]interface{}, len(args))
	for i, v := range args {
		interfaceArray[i] = v
	}

	logStr = getLogInfoByErrorCode(code)
	logStr = fmt.Sprintf(logStr, interfaceArray...)
	return logStr
}