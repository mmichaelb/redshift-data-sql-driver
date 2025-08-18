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

type redshiftDataTxNonTransactional struct{}

func (tx *redshiftDataTxNonTransactional) Commit() error {
	defer transactionMutex.Unlock()
	debugLogger.Printf("tx commit called, but transactionMode is set to 'non-transactional'")
	return nil
}

func (tx *redshiftDataTxNonTransactional) Rollback() error {
	defer transactionMutex.Unlock()
	debugLogger.Printf("tx rollback called, but transactionMode is set to 'non-transactional'")
	return nil
}
