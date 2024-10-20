package go_database

// Logger is common database operations log implementation.
type Logger struct {
	callback func(format string, v ...interface{})
}

func (l *Logger) SetCallback(callback func(format string, v ...interface{})) {
	l.callback = callback
}

func (l *Logger) Printf(format string, v ...interface{}) {
	l.callback(format, v)
}
