package oss

import "testing"

func TestCheckOptions(t *testing.T) {
	plugin := &OssPlugin{}
	optin := &OssOptions{Bucket: "data", Endpoint: "eos.hubei-3.cmecloud.cn", OtherOpts: "-o max_stat_cache_size=0 -o allow_other", AkId: "1223455", AkSecret: "22334567"}
	plugin.checkOptions(optin)
}
