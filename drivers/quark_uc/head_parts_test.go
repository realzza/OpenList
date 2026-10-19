package quark

import (
	"testing"

	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
)

func TestDownHeadPartsReachLink(t *testing.T) {
	d, _ := fakeQuark(t, 0)
	d.DownHeadParts, d.DownHeadPartSize, d.DownLaunchParts = 4, 3, 10
	link, err := d.getDownloadLink(&File{Fid: "f1"})
	if err != nil {
		t.Fatal(err)
	}
	if link.HeadParts != 4 || link.HeadPartSize != 3*utils.MB {
		t.Fatalf("head parts %d x %d bytes, want 4 x 3 MB", link.HeadParts, link.HeadPartSize)
	}
	if link.LaunchParts != 10 {
		t.Fatalf("launch parts %d, want 10", link.LaunchParts)
	}
	if c := link.Clone(); c.HeadParts != 4 || c.HeadPartSize != 3*utils.MB || c.LaunchParts != 10 {
		t.Fatalf("Clone dropped the head or launch parts: %d x %d, launch %d", c.HeadParts, c.HeadPartSize, c.LaunchParts)
	}
}
