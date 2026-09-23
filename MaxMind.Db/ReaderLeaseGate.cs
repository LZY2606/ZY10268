#region

using System;
using System.Threading;

#endregion

namespace MaxMind.Db
{
    /// <summary>
    ///     Tracks in-flight batch operations so that <see cref="Reader.Dispose()" />
    ///     can block new batches and wait for running ones before the underlying
    ///     memory-mapped buffer is released. This guarantees that a batch view
    ///     never observes a disposed memory map.
    /// </summary>
    internal sealed class ReaderLeaseGate
    {
        private readonly object _sync = new();
        private readonly ManualResetEventSlim _noOutstandingLeases = new(true);
        private int _activeLeases;
        private bool _closing;

        /// <summary>
        ///     Acquire a lease for a batch operation. Throws once the reader
        ///     has started disposing.
        /// </summary>
        internal void Enter()
        {
            lock (_sync)
            {
                if (_closing)
                {
                    throw new ObjectDisposedException(nameof(Reader));
                }

                _activeLeases++;
                _noOutstandingLeases.Reset();
            }
        }

        /// <summary>
        ///     Release a lease previously acquired with <see cref="Enter" />.
        /// </summary>
        internal void Exit()
        {
            lock (_sync)
            {
                _activeLeases--;
                if (_activeLeases == 0)
                {
                    _noOutstandingLeases.Set();
                }
            }
        }

        /// <summary>
        ///     Prevent new leases and wait until all outstanding leases are
        ///     released. Must not be called from a thread holding a lease.
        /// </summary>
        internal void CloseAndWait()
        {
            lock (_sync)
            {
                _closing = true;
            }

            _noOutstandingLeases.Wait();
        }

        internal void DisposeHandle()
        {
            _noOutstandingLeases.Dispose();
        }
    }
}
