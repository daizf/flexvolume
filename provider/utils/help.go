package utils

import "fmt"

var (
	// VERSION should be updated by hand at each release
	VERSION = "v1.13.0"

	// GITCOMMIT will be overwritten automatically by the build system
	GITCOMMIT = "HEAD"
)

// PluginVersion
func PluginVersion() string {
	return VERSION
}

// Usage help
func Usage() {
	fmt.Printf("In K8s Mode: " +
		"Use binary file as the first parameter, and format support:\n" +
		"    plugin init: \n" +
		"    plugin attach: for ecloud disk plugin\n" +
		"    plugin detach: for ecloud disk plugin\n" +
		"    plugin mount:  for nas, pfs, oss plugin\n" +
		"    plugin umount: for nas, pfs, oss plugin\n\n" +
		"You can refer to K8s flexvolume docs: \n")
}
