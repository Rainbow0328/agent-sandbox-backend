from __future__ import annotations

from typing import Any


class SandboxBackendError(Exception):
    code = "sandbox_backend_error"

    def __init__(
        self,
        message: str | None = None,
        *,
        provider_name: str | None = None,
        provider_key: str | None = None,
        sandbox_id: str | None = None,
        sandbox_instance_id: str | None = None,
        operation: str | None = None,
        operation_id: str | None = None,
        actor_id: str | None = None,
        correlation_id: str | None = None,
        retryable: bool = False,
        idempotency_key: str | None = None,
        provider_error_code: str | None = None,
        provider_request_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.message = message or self.code
        self.provider_name = provider_name
        self.provider_key = provider_key
        self.sandbox_id = sandbox_id
        self.sandbox_instance_id = sandbox_instance_id
        self.operation = operation
        self.operation_id = operation_id
        self.actor_id = actor_id
        self.correlation_id = correlation_id
        self.retryable = retryable
        self.idempotency_key = idempotency_key
        self.provider_error_code = provider_error_code
        self.provider_request_id = provider_request_id
        self.details = details or {}
        super().__init__(self.message)


class SandboxNotFoundError(SandboxBackendError):
    code = "sandbox_not_found"


class SandboxStateError(SandboxBackendError):
    code = "sandbox_state_error"


class UnsupportedCapabilityError(SandboxBackendError):
    code = "unsupported_capability"


class FileNotFoundError(SandboxBackendError):
    code = "file_not_found"


class ConcurrentModificationError(SandboxBackendError):
    code = "concurrent_modification"


class CommandExecutionError(SandboxBackendError):
    code = "command_execution_error"


class ProviderError(SandboxBackendError):
    code = "provider_error"


class HistoryDatabaseError(SandboxBackendError):
    code = "history_database_error"


class HistoryConfigValidationError(SandboxBackendError):
    code = "history_config_validation"


class HistoryConfigConflictError(SandboxBackendError):
    code = "history_config_conflict"


class HistoryTransportError(SandboxBackendError):
    code = "history_transport_error"


class LocalPathNotFoundError(SandboxBackendError):
    code = "local_path_not_found"


class LocalPathNotAllowedError(SandboxBackendError):
    code = "local_path_not_allowed"


class UploadPolicyError(SandboxBackendError):
    code = "upload_policy_error"


class UploadTooManyFilesError(SandboxBackendError):
    code = "upload_too_many_files"


class UploadFileTooLargeError(SandboxBackendError):
    code = "upload_file_too_large"


class UploadTotalSizeExceededError(SandboxBackendError):
    code = "upload_total_size_exceeded"


class UploadSymlinkError(SandboxBackendError):
    code = "upload_symlink_error"


class UploadArchiveSecurityError(SandboxBackendError):
    code = "upload_archive_security_error"


class UploadConflictError(SandboxBackendError):
    code = "upload_conflict"


class UploadChecksumMismatchError(SandboxBackendError):
    code = "upload_checksum_mismatch"


class UploadAtomicCommitError(SandboxBackendError):
    code = "upload_atomic_commit"


class UploadPartialFailureError(SandboxBackendError):
    code = "upload_partial_failure"


class LockAcquisitionTimeoutError(SandboxBackendError):
    code = "lock_acquisition_timeout"


class CommandQueueTimeoutError(SandboxBackendError):
    code = "command_queue_timeout"


class SandboxLeaseExpiredError(SandboxBackendError):
    code = "sandbox_lease_expired"


class SandboxDeletingError(SandboxBackendError):
    code = "sandbox_deleting"


class HistoryStorageExhaustedError(SandboxBackendError):
    code = "history_storage_exhausted"


class BackendCloseError(SandboxBackendError):
    code = "backend_close_error"

    def __init__(self, errors: list[BaseException]) -> None:
        self.errors = tuple(errors)
        summary = "; ".join(
            f"{type(error).__name__}: {error}" for error in self.errors
        )
        super().__init__(
            f"Backend close encountered {len(self.errors)} error(s): {summary}",
            details={
                "errors": [
                    {"type": type(error).__name__, "message": str(error)}
                    for error in self.errors
                ]
            },
        )
