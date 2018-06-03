package mta

//MTAStorageManager struct
type MTAStorageManager struct {
	messageQueue     chan *Message
	scope            string
	scanIntervalSecs int
	fileWatcher      *FileWatcher
	backupService    *BackupService
	index            *Index
}

//Init initialize MTA Storage Manager
func (mtaStorageManager *MTAStorageManager) Init() {
	mtaStorageManager.messageQueue = make(chan *Message)

	mtaStorageManager.index = &Index{
		q:    mtaStorageManager.messageQueue,
		file: "files.index",
	}

	mtaStorageManager.index.Init()

	mtaStorageManager.fileWatcher = &FileWatcher{
		q:                mtaStorageManager.messageQueue,
		scope:            mtaStorageManager.scope,
		scanIntervalSecs: mtaStorageManager.scanIntervalSecs,
	}

	mtaStorageManager.fileWatcher.Init()

	mtaStorageManager.backupService = &BackupService{
		q: mtaStorageManager.messageQueue,
	}
	mtaStorageManager.backupService.Init()

	go mtaStorageManager.fileWatcher.StartWatching()
	go mtaStorageManager.backupService.StartCron()
	go mtaStorageManager.startProcessing()
}

func (mtaStorageManager *MTAStorageManager) startProcessing() {
	for {
		msg := <-mtaStorageManager.messageQueue
		switch msg.msg {
		case "list_files":
			go mtaStorageManager.index.ListFiles(msg)
		case "search_files":
			go mtaStorageManager.index.SearchFiles(msg)
		case "backup_file":
			go mtaStorageManager.backupService.BackupFile(msg)
		case "update_file_info":
			go mtaStorageManager.index.UpdateFileInfo(msg)
		}
	}
}
