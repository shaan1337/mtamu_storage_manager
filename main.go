package mta

import "time"

func main() {
	mtaStorageManager := &MTAStorageManager{
		Scope:                 "/home/shaan/Downloads",
		FileScanIntervalSecs:  15,
		IndexScanIntervalSecs: 15,
	}

	mtaStorageManager.Init()
	for {
		time.Sleep(time.Millisecond * 10)
	}

}
