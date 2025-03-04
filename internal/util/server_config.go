package util

const S3Backup string = "s3"
const FSBackup string = "fs"

// ServerConfig represents configuration data needed to setup queue backup
type ServerConfig struct {
	BackupFrequency int
	BackupBucket    string
	BackupType      string
}
