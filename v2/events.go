package batcher

const (
	BatchEvent             = "batch"
	PauseEvent             = "pause"
	ResumeEvent            = "resume"
	ShutdownEvent          = "shutdown"
	AuditPassEvent         = "audit-pass"
	AuditFailEvent         = "audit-fail"
	AuditSkipEvent         = "audit-skip"
	RequestEvent           = "request"
	CapacityEvent          = "capacity"
	ReleasedEvent          = "released"
	AllocatedEvent         = "allocated"
	TargetEvent            = "target"
	VerifiedContainerEvent = "verified-container"
	CreatedContainerEvent  = "created-container"
	VerifiedBlobEvent      = "verified-blob"
	CreatedBlobEvent       = "created-blob"
	FailedEvent            = "failed"
	ErrorEvent             = "error"
)
