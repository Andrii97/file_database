package main

import (
	"encoding/xml"
	"io/ioutil"
	"sync"
	"fmt"
	"net"
	"os"
	"strings"
)

const (
	CONN_HOST = "localhost"
	CONN_PORT = "8888"
	CONN_TYPE = "tcp"
)

var (
	gtm sync.Mutex
)

type Cache []*Table

type Table struct {
	XMLName xml.Name `xml:"element"`
	name string
	data []element `xml:"element"`
	m sync.RWMutex
}

type element struct {
	XMLName xml.Name `xml:"element"`
	Key     string   `xml:"key"`
	Value   string   `xml:"value"`
}

func NewTable(file_name string) *Table {
	return &Table{name: file_name, data:make([] element, 0)}
}

func parse_xml(file_name string) *Table {
	data, err := ioutil.ReadFile("db/" + file_name)
	if err != nil {
		return nil
	}

	table := NewTable(file_name)

	err = xml.Unmarshal(data, &table.data)
	fmt.Println(err)
	if err != nil {
		return nil 
	}
	return table
}

func save(tablechan <-chan Table){
	for {
		table := <-tablechan
		data, _ := xml.Marshal(table.data)

		f, err := os.Create("db/" + table.name)
	    checkErr(err)
	    defer f.Close()
	    _, err = f.Write(data)
	    checkErr(err)
	}
	
}

func checkErr(e error) {
	if e != nil {
		panic(e)
	}
}

func getTable(tables *Cache, name string) *Table {
	gtm.Lock()
	for i := range *tables {
		if (*tables)[i].name == name {
			gtm.Unlock()
			return (*tables)[i]
		}
	}
	
	table := parse_xml(name)

	if table != nil {
		*tables = append(*tables, table)
	}
	gtm.Unlock()
	return table
}

func quit(c net.Conn) {
	c.Write([]byte(string("Bye\n")))
	c.Close()
}

func (table *Table) get_value(key string) string {
	for _, element := range table.data {
		if element.Key == key {
			return element.Value
		}
	}
	return ""
}

func (table *Table) set_value(key string, val string) {
	for i, element := range table.data {
		if element.Key == key {
			element.Value = val
			table.data[i] = element
			return
		}
	}
	element := element{XMLName: table.XMLName, Key: key, Value: val}
	table.data = append(table.data, element)
}

// without saving order
func remove_from_slice(s []element, i int) []element {
	s[len(s)-1], s[i] = s[i], s[len(s)-1]
	return s[:len(s)-1]
}

func (table *Table) del_key(key string) bool {
	for i, element := range table.data {
		if element.Key == key {
			table.data = remove_from_slice(table.data, i)
			return true
		}
	}
	return false
}

func getVal(c net.Conn, tables *Cache, query_split []string) {
	if len(query_split) == 3  {
		table := getTable(tables, query_split[0])
		if (table == nil) {
			c.Write([]byte(string("Unknown table\n")))
		} else {
			table.m.RLock()
			value := table.get_value(query_split[2])
			table.m.RUnlock()
			if value != "" {
				c.Write([]byte(string(value + "\n")))
			} else {
				c.Write([]byte(string("key does not exist\n")))
			}
		}
	} else {
		c.Write([]byte(string("Unknown command\n")))
	}
}

func setVal(c net.Conn, tablechan chan<- Table, tables *Cache, query_split []string) {
	if len(query_split) >= 4 {
		table := getTable(tables, query_split[0])
		if (table == nil) {
			table = &Table{name: query_split[0],data:make([] element, 0)}
		} 
		table.m.Lock()
		table.set_value(query_split[2], query_split[3])
		table.m.Unlock()
		c.Write([]byte(string("OK\n")))
		tablechan <- *table
	} else {
		c.Write([]byte(string("Unknown command\n")))
	}
}

func delKey(c net.Conn, tablechan chan<- Table, tables *Cache, query_split []string) {
	if len(query_split) == 3  {
		table := getTable(tables, query_split[0])
		if (table == nil) {
			c.Write([]byte(string("Unknown table\n")))
		} else {
			table.m.RLock()
			ok := table.del_key(query_split[2])
			table.m.RUnlock()
			if ok {
				c.Write([]byte(string("OK\n")))
				tablechan <- *table
			} else {
				c.Write([]byte(string("key does not exist\n")))
			}
		}
	} else {
		c.Write([]byte(string("Unknown command\n")))
	}
}


/*
	Patterns for query
	[table name] set [key] [value]
	[table name] del [key]
	quit
*/
func handleRequest(c net.Conn, tablechan chan<- Table, tables *Cache, query string) {
	query_split := strings.Fields(query)

	if len(query_split) >= 2 {
		switch strings.ToLower(query_split[1]) {
			case "set":
				setVal(c, tablechan, tables, query_split)
			case "get":
				getVal(c, tables, query_split)
			case "del":
				delKey(c, tablechan, tables, query_split)
			default:
				c.Write([]byte(string("Unknown command\n")))
		}
	} else if len(query_split) == 1 {
		switch strings.ToLower(query_split[0]) {
			case "q":
				quit(c)
			default:
				c.Write([]byte(string("Unknown command\n")))
			}
	} else {
		c.Write([]byte(string("Unknown command\n")))
	}
}

func handleConnection(c net.Conn, tablechan chan<- Table, tables *Cache) {
	buf := make([]byte, 4096)
	for {
		n, err := c.Read(buf)
		if (err != nil) || (n == 0) {
			break
		} else {
			go handleRequest(c, tablechan, tables, string(buf[0:n]))
		}
	}
}

func main() {
	ln, err := net.Listen(CONN_TYPE, CONN_HOST+":"+CONN_PORT)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer ln.Close()
	var tables Cache

	tablechan := make(chan Table)
	go save(tablechan) 
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}
		defer conn.Close()
		go handleConnection(conn, tablechan, &tables)
	}
}

