// ptocol creates a colour palette spaced evenly around the HSV circle.
// This is useful when you want to create palettes for pto3-web, to be
// served by ptosrv.
package main

import (
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
)

func hsvToRGB(h, s, v float64) (r float64, g float64, b float64) {
	// Algorithm from https://en.wikipedia.org/wiki/HSL_and_HSV#Converting_to_RGB
	c := s * v
	hPrime := h / 60.0
	x := c * (1 - math.Abs(math.Mod(hPrime, 2.0)-1))

	switch int(math.Ceil(hPrime)) {
	case 0:
		fallthrough
	case 1:
		r, g = c, x
	case 2:
		r, g = x, c
	case 3:
		g, b = c, x
	case 4:
		g, b = x, c
	case 5:
		r, b = x, c
	case 6:
		r, b = c, x
	}

	m := v - c

	return r + m, g + m, b + m
}

func toRGBInt(c float64) int {
	if c < 0.0 || c > 1.0 {
		log.Fatalf("colour value %g outside range 0.0..1.0", c)
	}

	return int(math.Floor(256.0 * c))
}

func rgbToWeb(r float64, g float64, b float64) string {
	return fmt.Sprintf("#%02x%02x%02x", toRGBInt(r), toRGBInt(g), toRGBInt(b))
}

func makeIntFromArg(arg string) int {
	ret, err := strconv.ParseInt(arg, 10, 32)
	if err != nil {
		log.Fatalf("Argument \"%s\" is not an integer", arg)
	}

	return int(ret)
}

func makeFloat64FromArg(arg string, purpose string, min, max float64) float64 {
	ret, err := strconv.ParseFloat(arg, 64)
	if err != nil {
		log.Fatalf("Argument \"%s\" (%s) is not an float", arg, purpose)
	}
	if ret < min {
		log.Fatalf("Argument \"%s\" (%s) is less than allowed minimum %g", arg, purpose, min)
	}
	if ret > max {
		log.Fatalf("Argument \"%s\" (%s) is greater than allowed maximum %g", arg, purpose, max)
	}

	return ret
}

func makeWheel(h float64, s float64, v float64, n int) {
	angle := 360.0 / float64(n)

	for i := 0; i < n; i++ {
		r, g, b := hsvToRGB(h, s, v)
		fmt.Println(rgbToWeb(r, g, b))
		h += angle
		if h > 360.0 {
			h -= 360.0
		}
	}
}

func main() {
	n := makeIntFromArg(os.Args[1])
	h := makeFloat64FromArg(os.Args[2], "hue", 0.0, 360.0)
	s := makeFloat64FromArg(os.Args[3], "saturation", 0.0, 1.0)
	v := makeFloat64FromArg(os.Args[4], "value", 0.0, 1.0)

	makeWheel(h, s, v, n)
}
