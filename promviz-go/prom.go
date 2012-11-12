package main

import (
	"os"
	"io"
	"bufio"
	"encoding/binary"
	"math"
	//"math/rand"
	"fmt"
)

type geopoint uint64
type direction uint16

type Point struct {
	pos geopoint
	elev float32 //meters

	// list of adjacent points, in clockwise or counter-clockwise order
	adjacent []*Point
}

type Tri struct {
	points [3]*Point
	
	angles [3]direction
	is_sloped bool
	gradient direction

	// adjacent[k] is the tri with the shared edge points[k:k+1]
	adjacent [3]*Tri
	/* this could be computed dynamically from the adjacency
	 * lists of the individual points, but since this is a very
	 * common operation, we pre-compute
	 */
}

type Mesh struct {
	points []Point
	tris []Tri

	points_to_tris map[[3]*Point]*Tri
}

type RasterSpec struct {
	x0 float64 // x-coordinate of top-left pixel (center of pixel)
	y0 float64 // y-coordinate of top-left pixel (center of pixel)
	dx float64 // width of pixel column
	dy float64 // height of pixel row
	width int  // total # of columns
	height int // total # of rows

	zunit float32 // scale of z units (in meters)
	nodata float64 // special value to be treated as 'no data'

	readSample func(io.Reader) float64 // read next sample from file buffer
	project func(float64, float64) (float64, float64) // convert x,y coords to lat/lon
}

func (rs *RasterSpec) NextSample(r io.Reader) float32 {
	val := rs.readSample(r)
	if math.IsNaN(val) || val == rs.nodata {
		return float32(math.NaN())
	}
	return float32(val) * rs.zunit
}

func (rs *RasterSpec) Load(r io.Reader) *Mesh {
	m := new(Mesh)
	num_points := rs.width * rs.height
	num_tris := 2 * (rs.width - 1) * (rs.height - 1)

	m.points = make([]Point, 0, num_points)
	m.tris = make([]Tri, 0, num_tris)
	m.points_to_tris = make(map[[3]*Point]*Tri, num_tris)

	index := make(map[[2]int]*Point, 3 * rs.width)

	for row, y := 0, rs.y0; row < rs.height; row, y = row + 1, y - rs.dy {
		// create points for cells in raster grid
		for col, x := 0, rs.x0; col < rs.width; col, x = col + 1, x + rs.dx {
			elev := rs.NextSample(r)
			if !math.IsNaN(float64(elev)) {
				lat, lon := rs.project(x, y)
				p := Point{pos: toGeopoint(lat, lon), elev: elev}

				m.points = append(m.points, p)
				index[[2]int{row, col}] = &p
			}
		}

		// create triangles
		if row > 0 {
			for col := 1; col < rs.width; col += 1 {
				var k1, k2 int
				if (row + col) % 2 == 0 {
					k1, k2 = -1, 0
				} else {
					k1, k2 = 0, -1
				}
				tri1 := [3][2]int{{-1, -1}, {0, -1}, {k1, 0}}
				tri2 := [3][2]int{{-1, 0}, {0, 0}, {k2, -1}}
				tris := [][3][2]int{tri1, tri2}

				for _, t := range tris {
					fmt.Printf("%v\n", t)
				}
			}
		}
	}

	return m
}

func toGeopoint(lat float64, lon float64) geopoint {
	return 0
}

func readInt16(r io.Reader) float64 {
	var val int16
	binary.Read(r, binary.LittleEndian, &val)
	return float64(val)
}

func geographicProjection(x float64, y float64) (lat float64, lon float64) {
	lat = y
	lon = x
	return
}

func loadDEM(path string, geom RasterSpec) *Mesh {
	f, err := os.Open(path)
    if err != nil { panic(err) }
	defer f.Close()

	r := bufio.NewReader(f)
	return geom.Load(r)
}

func tern(cond bool, a interface{}, b interface{}) interface{} {
	if cond {
		return a
	}
	return b
}

func main() {
	mesh := loadDEM("/tmp/data", RasterSpec{
		width: 6001,
		height: 501,
		zunit: 1.,
		nodata: math.NaN(),
		readSample: readInt16,
		project: geographicProjection,
	})
	_ = mesh

    fmt.Printf("%d %d\n", len(mesh.points), len(mesh.tris))
}