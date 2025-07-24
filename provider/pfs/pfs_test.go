package pfs

import "testing"

func TestCheckOptions(t *testing.T) {
	plugin := &PfsPlugin{}
	optin := &PfsOptions{Server: "aliyun", Path: "oss-cn-hangzhou.aliyuncs.com", SkId: "22334567"}
	plugin.checkOptions(optin)
}
