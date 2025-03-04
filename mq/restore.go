package mq

import (
	"github.com/ChukwuEmekaAjah/mq/internal/util"
)

// Restore tries to read the queue backup data into memory when the server starts
func Restore(store *Store, config *util.ServerConfig) {
	store.backupManager.Restore(store)
}
