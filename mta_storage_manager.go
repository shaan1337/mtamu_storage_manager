package mta

//MTAStorageManager struct
type MTAStorageManager struct {
	Scope                 string
	FileScanIntervalSecs  int
	IndexScanIntervalSecs int
	fileWatcher           *FileWatcher
	backupService         *BackupService
	index                 *Index
}

//Init initialize MTA Storage Manager
func (m *MTAStorageManager) Init() {
	m.index = &Index{
		file: "files.index",
	}
	m.index.Init()

	m.fileWatcher = &FileWatcher{
		scope:                 m.Scope,
		fileScanIntervalSecs:  m.FileScanIntervalSecs,
		indexScanIntervalSecs: m.IndexScanIntervalSecs,
		index: m.index,
	}
	m.fileWatcher.Init()

	m.backupService = &BackupService{}
	m.backupService.Init()

	go m.fileWatcher.StartWatching()
	go m.backupService.StartCron()
}

func (m *MTAStorageManager) UpdatePath(path string) {
	m.fileWatcher.ScanPath(path, 1)
}

func (m *MTAStorageManager) GetFileInfo(path string) (*FileInfo, error) {
	return m.index.GetFileInfo(path)
}

func (m *MTAStorageManager) GetDirectoryListing(dir string) (*DirListing, error) {
	return m.index.GetDirectoryListing(dir)
}
