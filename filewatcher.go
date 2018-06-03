package mta

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

//FileWatcher struct
type FileWatcher struct {
	q                chan<- *Message
	scope            string
	scanIntervalSecs int
}

//Init FileWatcher service
func (watcher *FileWatcher) Init() {
}

//StartWatching start watching for file changes
func (watcher *FileWatcher) StartWatching() {
	for {
		fmt.Printf("[filewatcher] Walking through %s\n", watcher.scope)
		inprocess := 0
		out := make(chan int)
		err := filepath.Walk(watcher.scope, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				fmt.Printf("[filewatcher] error : %s\n", err.Error())
				return err
			}
			//fmt.Printf("visiting file: %q\n", path)
			var params = make(map[string]interface{})
			params["path"] = path
			params["size"] = info.Size()
			params["dir"] = info.IsDir()
			params["modtime"] = info.ModTime()
			params["out"] = out

			var msg = &Message{
				msg:    "update_file_info",
				params: params,
			}
			watcher.q <- msg
			inprocess++

			return nil
		})

		if err != nil {
			fmt.Printf("[filewatcher] error : %s\n", err.Error())
		} else {
			fmt.Printf("[filewatcher] %d paths scheduled for processing\n", inprocess)
		}

		processed := 0
		for processed < inprocess {
			<-out
			processed++
			if processed%500 == 0 {
				fmt.Printf("[filewatcher] %d paths processed...\n", processed)
			}
		}

		fmt.Printf("[filewatcher] %d paths processed\n", inprocess)

		duration := time.Second * time.Duration(watcher.scanIntervalSecs)
		time.Sleep(duration)
	}
}
