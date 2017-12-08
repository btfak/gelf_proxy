package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"
)

const (
	ChunkSize = 1420
)

var (
	localAddr    = flag.String("local", ":12201", "local addr")
	forwardAddrs = flag.String("forward", "127.0.0.1:5000,127.0.0.1:6000", "forward addrs")
	forwardConns []*net.UDPConn

	chunkPacket  = make(chan *packet, 1024)
	normalPacket = make(chan *packet, 10240)

	magicChunk = []byte{0x1e, 0x0f}

	pool = &sync.Pool{
		New: func() interface{} {
			return &packet{data: make([]byte, ChunkSize)}
		},
	}
)

type packet struct {
	data []byte
	len  int
}

func main() {
	initial()
	go forward()
	err := listen(*localAddr)
	if err != nil {
		log.Fatal(err)
	}
}

func initial() {
	flag.Parse()
	rand.Seed(time.Now().Unix())
	log.Printf("listen %v and foward to %v\n", *localAddr, *forwardAddrs)
	addrs := strings.Split(*forwardAddrs, ",")
	for _, addr := range addrs {
		udpAddr, err := net.ResolveUDPAddr("udp", strings.TrimSpace(addr))
		if err != nil {
			log.Fatal(err)
		}
		conn, err := net.DialUDP("udp", nil, udpAddr)
		if err != nil {
			log.Fatal("Can't dial: ", err)
		}
		forwardConns = append(forwardConns, conn)
	}
	if len(forwardConns) == 0 {
		log.Fatal("empty forward conns")
	}
}

func forward() {
	go func() {
		for packet := range chunkPacket {
			_, err := pick(packet.data[2 : 2+8]).Write(packet.data[:packet.len])
			if err != nil {
				log.Println(err)
			}
			pool.Put(packet)
		}
	}()

	for i := 0; i < 10; i++ {
		go func() {
			for packet := range normalPacket {
				_, err := pick(nil).Write(packet.data[:packet.len])
				if err != nil {
					log.Println(err)
				}
				pool.Put(packet)
			}
		}()
	}
}

func pick(cid []byte) *net.UDPConn {
	if len(cid) == 0 {
		return forwardConns[rand.Intn(len(forwardConns))]
	}
	return forwardConns[int(cid[len(cid)-1])%len(forwardConns)]
}

func listen(addr string) error {
	var err error
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return fmt.Errorf("ResolveUDPAddr('%s'): %s", addr, err)
	}

	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return fmt.Errorf("ListenUDP: %s", err)
	}
	conn.SetReadBuffer(50 * 1024 * 1024)
	read(conn)
	return nil
}

func read(conn *net.UDPConn) {
	for {
		pkt := pool.Get().(*packet)
		n, err := conn.Read(pkt.data)
		if err != nil {
			continue
		}
		pkt.len = n
		if bytes.Equal(pkt.data[:2], magicChunk) {
			chunkPacket <- pkt
			continue
		}
		normalPacket <- pkt
	}
}
