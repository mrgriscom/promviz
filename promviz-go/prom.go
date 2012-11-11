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
	x0 float64
	y0 float64
	dx float64
	dy float64
	width int
	height int

	zunit float32
	nodata float64

	readSample func(io.Reader) float64
	project func(float64, float64) (float64, float64)
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
	m.points = make([]Point, 0, rs.width * rs.height)

	for row, y := 0, rs.y0; row < rs.height; row, y = row + 1, y - rs.dy {
		for col, x := 0, rs.x0; col < rs.width; col, x = col + 1, x + rs.dx {
			elev := rs.NextSample(r)
			if !math.IsNaN(float64(elev)) {
				lat, lon := rs.project(x, y)
				p := Point{pos: toGeopoint(lat, lon), elev: elev}
				m.points = append(m.points, p)
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

func main() {
	mesh := loadDEM("/tmp/data", RasterSpec{
		width: 6001,
		height: 6001,
		zunit: 1.,
		nodata: math.NaN(),
		readSample: readInt16,
		project: geographicProjection,
	})
	_ = mesh

    fmt.Printf("hello, world\n")
}