package mta

import "time"

func main() {
	mtaStorageManager := &MTAStorageManager{
		scope:            "/home/shaan/Downloads",
		scanIntervalSecs: 15,
	}

	mtaStorageManager.Init()
	for {
		time.Sleep(time.Millisecond * 10)
	}

}
