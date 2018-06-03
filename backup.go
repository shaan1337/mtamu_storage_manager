package mta

//BackupService struct
type BackupService struct {
	q chan<- *Message
}

//Init backup service
func (backup *BackupService) Init() {
}

//StartCron activate cron jobs
func (backup *BackupService) StartCron() {
}

//BackupFile backup specific file/directory
func (backup *BackupService) BackupFile(msg *Message) {
	//file := msg.params["file"].(string)
}
