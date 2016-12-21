package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"net"
	"os"
	"regexp"
	"strings"
	"sync"
)

const (
	CONN_HOST = "localhost"
	CONN_PORT = "8888"
	CONN_TYPE = "tcp"
)

type table struct {
	name     string
	data     *[][]string
	isLocked sync.Mutex
}

type cache struct {
	tables []table
}

func handleRequest(command string, c *cache, conn net.Conn) {
	fmt.Println("handle request: " + command)

	params := strings.Fields(command)
	// remove empty entries and remove whitespaces
	fmt.Println(params)
	fmt.Println(params[0])
	// remove empty entries and remove whitespaces
	switch strings.ToLower(params[0]) {
	case "exit":
		os.Exit(0)

		// TODO: patterns for queries

	default:
		conn.Write([]byte(string("Your command didn't match the pattern\n")))
	}
}

// Handles incoming requests.
func handleConnection(conn net.Conn, c *cache) {
	// Make a buffer to hold incoming data.
	var buf [512]byte
	empty_string := ""
	for {
		copy(buf[:], empty_string) //make  buffer empty
		_, err := conn.Read(buf[0:])
		if err != nil {
			return
		}
		n := bytes.Index(buf[:], []byte{0})
		s := string(buf[:n])
		go handleRequest(s, c, conn)
	}
}

func main() {
	// Listen for incoming connections.
	c := cache{}
	l, err := net.Listen(CONN_TYPE, CONN_HOST+":"+CONN_PORT)
	if err != nil {
		fmt.Println("Error listening:", err.Error())
		os.Exit(1)
	}
	// Close the listener when the application closes.
	defer l.Close()
	fmt.Println("Listening on " + CONN_HOST + ":" + CONN_PORT)
	for {
		// Listen for an incoming connection.
		conn, err := l.Accept()
		fmt.Println("accept")

		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			os.Exit(1)
		}
		// Handle connections in a new goroutine.
		go handleConnection(conn, &c)
	}
}
