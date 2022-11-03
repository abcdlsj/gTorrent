package main

import (
	"gTorrent/internal"
	"os"
)

func main() {
	filePath := os.Args[1]
	meta, err := internal.Open(filePath)
	if err != nil {
		panic(err)
	}
	t := meta.Torrent()
	t.RequestTracker()
	t.Detail() // debug
	buf := t.Download()
	file, err := os.Create(t.Name)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	file.Write(buf)
}
