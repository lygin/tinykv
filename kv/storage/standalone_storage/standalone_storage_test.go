package standalone_storage

import (
	"fmt"
	"testing"
)

func TestNeedTest(t *testing.T) {
	s := "start tes"
	if s == "start test" {
		fmt.Println("1")
	} else if s == "start test!" {
		fmt.Println("2")
	} else {
		fmt.Println("3");
	}
	NeedTest(s);
}
