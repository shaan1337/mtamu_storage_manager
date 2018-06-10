package mta

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/shaan1337/mtamu_storage_manager/fileutils"
)

//FileWatcher struct
type FileWatcher struct {
	scope                 string
	fileScanIntervalSecs  int
	indexScanIntervalSecs int
	index                 *Index
}

var (
	scanInProgress bool
)

//Init FileWatcher service
func (fw *FileWatcher) Init() {
}

//StartWatching start watching for file changes
func (fw *FileWatcher) StartWatching() {
	go fw.startPeriodicFileScan()
	go fw.startPeriodicIndexScan()
}

func (fw *FileWatcher) startPeriodicFileScan() {
	for {
		for scanInProgress {
			time.Sleep(time.Second * 1)
		}
		scanInProgress = true
		fw.ScanPath(fw.scope, math.MaxInt32)
		scanInProgress = false
		duration := time.Second * time.Duration(fw.fileScanIntervalSecs)
		time.Sleep(duration)
	}
}

func (fw *FileWatcher) startPeriodicIndexScan() {
	for {
		for scanInProgress {
			time.Sleep(time.Second * 1)
		}
		scanInProgress = true
		fw.ScanIndex()
		scanInProgress = false
		duration := time.Second * time.Duration(fw.indexScanIntervalSecs)
		time.Sleep(duration)
	}
}

func (fw *FileWatcher) ScanPath(root string, maxLevel int) {
	fmt.Printf("[filewatcher] Walking through %s\n", root)
	inprocess := 0
	out := make(chan bool)
	err := fileutils.Walk(root, maxLevel, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			fmt.Printf("[filewatcher] error : %s\n", err.Error())
			return err
		}

		fileinfo := &FileInfo{
			Name:       info.Name(),
			Path:       path,
			ParentPath: filepath.Dir(path),
			Size:       info.Size(),
			IsDir:      info.IsDir(),
			ModTime:    info.ModTime(),
			Mode:       info.Mode(),
			ModeString: info.Mode().String(),
		}
		go fw.index.UpdateFileInfo(path, fileinfo, out)
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
}

func (fw *FileWatcher) ScanIndex() {
	fmt.Printf("[filewatcher] Scanning index\n")
	page := 1
	for {
		fileInfos, hasMore, err := fw.index.ListFiles(page)
		if err != nil {
			fmt.Printf("[filewatcher] error : %s\n", err.Error())
			break
		}

		for _, fileInfo := range fileInfos {
			_, err := os.Stat(fileInfo.Path)
			if err != nil {
				err1 := fw.index.DeleteFileInfo(fileInfo.Path)
				if err1 != nil {
					fmt.Printf("[filewatcher] error deleting %s from index: %s\n", fileInfo.Path, err.Error())
				}
			}
		}
		if !hasMore {
			break
		}
		page++
	}
}
