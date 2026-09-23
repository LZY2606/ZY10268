#region

using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

#endregion

namespace MaxMind.Db
{
    public sealed partial class Reader
    {
        // Slot states for batch pointer resolution.
        private const byte SlotUnresolved = 0;
        private const byte SlotResolved = 1;
        private const byte SlotFaulted = 2;

        private struct BatchPointerSlot
        {
            internal long Pointer;
            internal int PrefixLength;
            internal byte State;
            internal InvalidDatabaseException? Error;
        }

        // Per-worker tree-walk state, reused across all addresses a worker
        // processes so that no per-record scratch is allocated. The cache
        // lets duplicate addresses share their lookup path; each duplicate
        // still decodes independently, so no mutable activator state is ever
        // shared between results.
        private sealed class BatchWorkerState
        {
            internal readonly byte[] Scratch = new byte[16];
            internal readonly Dictionary<IPAddress, (long Pointer, int PrefixLength)> Cache = new();
        }

        /// <summary>
        ///     Looks up a batch of addresses, invoking
        ///     <paramref name="projection" /> once per address, strictly in
        ///     input order. The metadata, tree-walk scratch, and decoder are
        ///     reused across the whole batch instead of being re-established
        ///     per record, and duplicate addresses share their search-tree
        ///     resolution.
        /// </summary>
        /// <typeparam name="TResult">The projected result type.</typeparam>
        /// <param name="addresses">The addresses to look up.</param>
        /// <param name="results">
        ///     The destination for the projected results. Must be at least as
        ///     long as <paramref name="addresses" />. Supplying the
        ///     destination lets callers reuse buffers across batches.
        /// </param>
        /// <param name="projection">
        ///     Called once per address, in input order, with a
        ///     <see cref="BatchLookup" /> view that is valid only for the
        ///     duration of the call.
        /// </param>
        /// <param name="options">
        ///     Optional parallelism settings. Parallelism never changes the
        ///     result order, the first error surfaced, or the cancellation
        ///     position.
        /// </param>
        /// <param name="cancellationToken">
        ///     When cancellation is observed, the address at that position
        ///     and all subsequent addresses are reported to the projection
        ///     with <see cref="BatchLookupStatus.Cancelled" />; results for
        ///     earlier addresses are unaffected. The position is deterministic
        ///     and does not depend on the degree of parallelism.
        /// </param>
        /// <remarks>
        ///     A corrupt search tree is reported per address as
        ///     <see cref="BatchLookupStatus.Faulted" />. A corrupt data record
        ///     surfaces as an <see cref="InvalidDatabaseException" /> from
        ///     <see cref="BatchLookup.Decode{T}" />, matching
        ///     <see cref="Find{T}(IPAddress, InjectableValues?)" />; because
        ///     projections run in input order, the first such exception is
        ///     always the one for the lowest corrupt index.
        ///     <para>
        ///         While a batch is in flight, <see cref="Dispose()" /> blocks
        ///         until the batch completes. Do not call
        ///         <see cref="Dispose()" /> from within the projection callback.
        ///     </para>
        /// </remarks>
        /// <exception cref="ArgumentNullException">
        ///     <paramref name="projection" /> is null.
        /// </exception>
        /// <exception cref="ArgumentException">
        ///     <paramref name="results" /> is shorter than
        ///     <paramref name="addresses" />.
        /// </exception>
        /// <exception cref="ArgumentOutOfRangeException">
        ///     <see cref="BatchLookupOptions.MaxDegreeOfParallelism" /> is less
        ///     than 1.
        /// </exception>
        /// <exception cref="ObjectDisposedException">
        ///     The reader has been disposed.
        /// </exception>
        public void FindBatch<TResult>(
            ReadOnlySpan<IPAddress> addresses,
            Span<TResult> results,
            BatchProjection<TResult> projection,
            BatchLookupOptions? options = null,
            CancellationToken cancellationToken = default)
        {
            if (projection is null)
            {
                throw new ArgumentNullException(nameof(projection));
            }

            if (results.Length < addresses.Length)
            {
                throw new ArgumentException(
                    "The results span must be at least as long as the addresses span.",
                    nameof(results));
            }

            var maxDegreeOfParallelism = options?.MaxDegreeOfParallelism ?? 1;
            if (maxDegreeOfParallelism < 1)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(options),
                    "MaxDegreeOfParallelism must be at least 1.");
            }

            _batchLeaseGate.Enter();
            try
            {
                if (maxDegreeOfParallelism > 1 && addresses.Length > 1)
                {
                    // The span cannot be captured by the parallel loop, so
                    // the addresses are copied once per batch.
                    var addressArray = addresses.ToArray();
                    var slots = new BatchPointerSlot[addressArray.Length];
                    ResolvePointersInParallel(
                        addressArray, slots, maxDegreeOfParallelism, cancellationToken);
                    ReplayBatch(addressArray, slots, results, projection, cancellationToken);
                }
                else
                {
                    ReplayBatchSequential(addresses, results, projection, cancellationToken);
                }
            }
            finally
            {
                _batchLeaseGate.Exit();
            }
        }

        /// <summary>
        ///     Looks up a batch of addresses from an asynchronous sequence,
        ///     invoking <paramref name="projection" /> once per address,
        ///     strictly in input order, and returns the projected results in
        ///     the same order.
        /// </summary>
        /// <typeparam name="TResult">The projected result type.</typeparam>
        /// <param name="addresses">The addresses to look up.</param>
        /// <param name="projection">
        ///     Called once per address, in input order, with a
        ///     <see cref="BatchLookup" /> view that is valid only for the
        ///     duration of the call.
        /// </param>
        /// <param name="cancellationToken">
        ///     When cancellation is observed, iteration stops and the results
        ///     for the addresses processed so far are returned. The number of
        ///     results is the deterministic cancellation position.
        /// </param>
        /// <returns>
        ///     The projected results in input order. When cancellation stops
        ///     the batch early, only the results before the cancellation
        ///     position are included.
        /// </returns>
        /// <remarks>
        ///     While a batch is in flight, <see cref="Dispose()" /> blocks
        ///     until the batch completes. Do not call <see cref="Dispose()" />
        ///     from within the projection callback.
        /// </remarks>
        /// <exception cref="ArgumentNullException">
        ///     <paramref name="addresses" /> or <paramref name="projection" />
        ///     is null.
        /// </exception>
        /// <exception cref="ObjectDisposedException">
        ///     The reader has been disposed.
        /// </exception>
        public async Task<IReadOnlyList<TResult>> FindBatchAsync<TResult>(
            IAsyncEnumerable<IPAddress> addresses,
            BatchProjection<TResult> projection,
            CancellationToken cancellationToken = default)
        {
            if (addresses is null)
            {
                throw new ArgumentNullException(nameof(addresses));
            }

            if (projection is null)
            {
                throw new ArgumentNullException(nameof(projection));
            }

            _batchLeaseGate.Enter();
            try
            {
                var results = new List<TResult>();
                var worker = new BatchWorkerState();
                await using var enumerator = addresses.GetAsyncEnumerator(cancellationToken);
                while (true)
                {
                    bool moved;
                    try
                    {
                        moved = await enumerator.MoveNextAsync().ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                        break;
                    }

                    if (!moved || cancellationToken.IsCancellationRequested)
                    {
                        break;
                    }

                    var address = enumerator.Current;
#if NETSTANDARD2_0
                    var slot = ResolveAddress(address, worker);
#else
                    var slot = ResolveAddress(address, worker.Scratch, worker);
#endif
                    var lookup = CreateLookup(address, slot);
                    results.Add(projection(lookup));
                }

                return results;
            }
            finally
            {
                _batchLeaseGate.Exit();
            }
        }

        // Single-lookup decode shared with the batch view. Kept as a separate
        // internal entry point so the public BatchLookup type can decode
        // without exposing the private pointer resolution.
        internal T DecodeBatchPointer<T>(long pointer, InjectableValues? injectables, Network? network)
            where T : class
        {
            return ResolveDataPointer<T>(pointer, injectables, network);
        }

        private void ReplayBatchSequential<TResult>(
            ReadOnlySpan<IPAddress> addresses,
            Span<TResult> results,
            BatchProjection<TResult> projection,
            CancellationToken cancellationToken)
        {
            var worker = new BatchWorkerState();
#if !NETSTANDARD2_0
            Span<byte> scratch = stackalloc byte[16];
#endif
            var cancelled = false;
            for (var i = 0; i < addresses.Length; i++)
            {
                BatchLookup lookup;
                if (cancelled || cancellationToken.IsCancellationRequested)
                {
                    cancelled = true;
                    lookup = CancelledLookup(addresses[i]);
                }
                else
                {
#if NETSTANDARD2_0
                    var slot = ResolveAddress(addresses[i], worker);
#else
                    var slot = ResolveAddress(addresses[i], scratch, worker);
#endif
                    lookup = CreateLookup(addresses[i], slot);
                }

                results[i] = projection(lookup);
            }
        }

        private void ReplayBatch<TResult>(
            IPAddress[] addresses,
            BatchPointerSlot[] slots,
            Span<TResult> results,
            BatchProjection<TResult> projection,
            CancellationToken cancellationToken)
        {
            var cancelled = false;
            for (var i = 0; i < addresses.Length; i++)
            {
                BatchLookup lookup;
                if (cancelled || cancellationToken.IsCancellationRequested)
                {
                    cancelled = true;
                    lookup = CancelledLookup(addresses[i]);
                }
                else
                {
                    lookup = CreateLookup(addresses[i], slots[i]);
                }

                results[i] = projection(lookup);
            }
        }

        private void ResolvePointersInParallel(
            IPAddress[] addresses,
            BatchPointerSlot[] slots,
            int maxDegreeOfParallelism,
            CancellationToken cancellationToken)
        {
            var parallelOptions = new ParallelOptions
            {
                MaxDegreeOfParallelism = maxDegreeOfParallelism
            };
            Parallel.For(
                0,
                addresses.Length,
                parallelOptions,
                () => new BatchWorkerState(),
                (i, state, worker) =>
                {
                    if (state.IsStopped || cancellationToken.IsCancellationRequested)
                    {
                        state.Stop();
                        return worker;
                    }

#if NETSTANDARD2_0
                    slots[i] = ResolveAddress(addresses[i], worker);
#else
                    slots[i] = ResolveAddress(addresses[i], worker.Scratch, worker);
#endif
                    return worker;
                },
                _ => { });
        }

#if NETSTANDARD2_0
        private BatchPointerSlot ResolveAddress(IPAddress address, BatchWorkerState worker)
        {
            if (worker.Cache.TryGetValue(address, out var cached))
            {
                return new BatchPointerSlot
                {
                    Pointer = cached.Pointer,
                    PrefixLength = cached.PrefixLength,
                    State = SlotResolved
                };
            }

            try
            {
                var rawAddress = address.GetAddressBytes();
                rawAddress.CopyTo(worker.Scratch, 0);
                var pointer = FindAddressInTree(worker.Scratch, rawAddress.Length * 8, out var prefixLength);
                worker.Cache.Add(address, (pointer, prefixLength));
                return new BatchPointerSlot
                {
                    Pointer = pointer,
                    PrefixLength = prefixLength,
                    State = SlotResolved
                };
            }
            catch (InvalidDatabaseException ex)
            {
                return new BatchPointerSlot { State = SlotFaulted, Error = ex };
            }
        }
#else
        private BatchPointerSlot ResolveAddress(IPAddress address, Span<byte> scratch, BatchWorkerState worker)
        {
            if (worker.Cache.TryGetValue(address, out var cached))
            {
                return new BatchPointerSlot
                {
                    Pointer = cached.Pointer,
                    PrefixLength = cached.PrefixLength,
                    State = SlotResolved
                };
            }

            try
            {
                var rawAddress = WriteAddressBytes(address, scratch);
                var pointer = FindAddressInTree(rawAddress, out var prefixLength);
                worker.Cache.Add(address, (pointer, prefixLength));
                return new BatchPointerSlot
                {
                    Pointer = pointer,
                    PrefixLength = prefixLength,
                    State = SlotResolved
                };
            }
            catch (InvalidDatabaseException ex)
            {
                return new BatchPointerSlot { State = SlotFaulted, Error = ex };
            }
        }
#endif

        private BatchLookup CreateLookup(IPAddress address, BatchPointerSlot slot)
        {
            switch (slot.State)
            {
                case SlotResolved:
                    var status = slot.Pointer == 0
                        ? BatchLookupStatus.NotFound
                        : BatchLookupStatus.Found;
                    return new BatchLookup(
                        this, address, status, slot.Pointer, slot.PrefixLength,
                        IsIPv4InIPv6(address), slot.Error);

                case SlotFaulted:
                    return new BatchLookup(
                        this, address, BatchLookupStatus.Faulted, 0, 0,
                        IsIPv4InIPv6(address), slot.Error);

                default:
                    // Unresolved slots only remain when parallel resolution
                    // stopped early after cancellation; the ordered replay
                    // reports them as cancelled.
                    return CancelledLookup(address);
            }
        }

        private BatchLookup CancelledLookup(IPAddress address)
        {
            return new BatchLookup(
                this, address, BatchLookupStatus.Cancelled, 0, 0,
                IsIPv4InIPv6(address), null);
        }

        private bool IsIPv4InIPv6(IPAddress address)
        {
            return _dbIPVersion == 6
                   && address.AddressFamily == AddressFamily.InterNetwork;
        }
    }
}
