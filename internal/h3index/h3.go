package h3index

import (
	"fmt"

	h3 "github.com/uber/h3-go/v4"
)

const Resolution = 7

func Cell(lat, lng float64) (h3.Cell, error) {
	return h3.NewLatLng(lat, lng).Cell(Resolution)
}

// GridDiskK1 returns the origin cell and its immediate neighbors (k-ring with k=1).
func GridDiskK1(cell h3.Cell) ([]h3.Cell, error) {
	return cell.GridDisk(1)
}

func CellKey(c h3.Cell) string {
	return fmt.Sprintf("%d", uint64(c))
}
