import Foundation
import NIOConcurrencyHelpers

/// Cancellation-aware async semaphore.
///
/// Implemented as a lock-protected class so it can be called from `@Sendable`
/// cancellation handlers without crossing actor-isolated state.
/// Use `wait()` to acquire a permit (suspending if none are available) and
/// `signal()` to release one.
@available(macOS 10.15, iOS 13, tvOS 13, watchOS 6, *)
public final class AsyncSemaphore: @unchecked Sendable {
    private let lock = NIOLock()
    private var available: Int
    private var waiters: [(id: UUID, continuation: CheckedContinuation<Void, any Error>)] = []
    private var headIndex = 0

    /// Creates a semaphore with the given initial number of available permits.
    ///
    /// - Parameter value: The initial permit count. Negative values are treated as `0`.
    public init(value: Int) {
        self.available = max(0, value)
    }

    /// Acquires a permit, suspending until one is available or the task is cancelled.
    ///
    /// - Throws: `CancellationError` if the current task is cancelled while waiting.
    public func wait() async throws {
        if Task.isCancelled { throw CancellationError() }

        if tryAcquire() {
            return
        }

        let id = UUID()
        try await withTaskCancellationHandler(
            operation: {
                try await withCheckedThrowingContinuation { continuation in
                    var shouldResumeImmediately = false
                    lock.withLock {
                        if available > 0 {
                            available -= 1
                            shouldResumeImmediately = true
                        } else {
                            waiters.append((id: id, continuation: continuation))
                        }
                    }

                    if shouldResumeImmediately {
                        continuation.resume()
                    }
                }
            },
            onCancel: { [weak self] in
                self?.cancelWaiter(id: id)
            }
        )
    }

    /// Releases a permit, resuming one waiting task if present.
    public func signal() {
        lock.withLock {
            if headIndex > 32 && headIndex * 2 > waiters.count {
                waiters.removeFirst(headIndex)
                headIndex = 0
            }
        }

        while true {
            let next: CheckedContinuation<Void, any Error>?
            next = lock.withLock {
                while headIndex < waiters.count {
                    let waiter = waiters[headIndex]
                    headIndex += 1
                    return waiter.continuation
                }
                available += 1
                return nil
            }

            guard let cont = next else { return }
            cont.resume()
            return
        }
    }

    private func tryAcquire() -> Bool {
        lock.withLock {
            if available > 0 {
                available -= 1
                return true
            }
            return false
        }
    }

    private func cancelWaiter(id: UUID) {
        let cont: CheckedContinuation<Void, any Error>? = lock.withLock {
            for idx in headIndex..<waiters.count where waiters[idx].id == id {
                let waiter = waiters[idx]
                waiters.remove(at: idx)
                return waiter.continuation
            }
            return nil
        }
        cont?.resume(throwing: CancellationError())
    }
}
