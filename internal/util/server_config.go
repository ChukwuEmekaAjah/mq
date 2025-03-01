package util

// ServerConfig represents configuration data needed to setup queue backup
type ServerConfig struct {
	BackupFrequency int
	BackupBucket    string
	BackupType      string
}
