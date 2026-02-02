import Foundation
import NIOConcurrencyHelpers

/// Cancellation-aware async semaphore.
///
/// Implemented as a lock-protected class so it can be used from `@Sendable`
/// cancellation handlers without crossing actor isolation boundaries.
public final class AsyncSemaphore: @unchecked Sendable {
    private let lock = NIOLock()
    private var available: Int
    private var waiters: [(id: UUID, continuation: CheckedContinuation<Void, any Error>)] = []
    private var headIndex = 0

    public init(value: Int) {
        self.available = max(0, value)
    }

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
