package redshiftdatasqldriver

type redshiftDataTxEmulated struct {
	onCommit   func() error
	onRollback func() error
}

func (tx *redshiftDataTxEmulated) Commit() error {
	debugLogger.Printf("tx commit called")
	return tx.onCommit()
}

func (tx *redshiftDataTxEmulated) Rollback() error {
	debugLogger.Printf("tx rollback called")
	return tx.onRollback()
}
