package ahrw_test

import (
	"fmt"

	"github.com/SenseUnit/ahrw"
)

func Example() {
	nodes := []ahrw.Node{
		ahrw.NewServer("server1", "red"),
		ahrw.NewServer("server2", "orange"),
		ahrw.NewServer("server3", "yellow"),
		ahrw.NewServer("server4", "green"),
		ahrw.NewServer("server5", "blue"),
		ahrw.NewServer("server6", "indigo"),
		ahrw.NewServer("server7", "violet"),
	}
	h, err := ahrw.New(16384, nodes)
	if err != nil {
		panic(err)
	}
	for _, obj := range []string{"object1", "object2", "object3", "object4", "object5"} {
		fmt.Printf("%s -> %s\n", obj, h.NodeForString(obj).(*ahrw.Server).Handle().(string))
	}
}
