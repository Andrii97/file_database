// parse
package main

import (
	"encoding/xml"
	"fmt"
	"io/ioutil"
)

type table struct {
	XMLName xml.Name `xml:"Data"`
	Try     string   `xml:"Try"`
}

func parse_xml(name string) *table {
	key_val, err := ioutil.ReadFile(name)
	if err != nil {
		return nil
	}

	tab := &table{}

	err = xml.Unmarshal(key_val, tab)
	if err != nil {
		return nil
	}
	return tab
}

func main() {
	p := parse_xml("my.xml")
	fmt.Println(p)
}
