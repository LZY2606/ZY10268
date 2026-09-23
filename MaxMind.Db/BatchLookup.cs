#region

using System;
using System.Net;
using System.Runtime.ExceptionServices;

#endregion

namespace MaxMind.Db
{
    /// <summary>
    ///     The outcome of a single address within a batch lookup.
    /// </summary>
    public enum BatchLookupStatus
    {
        /// <summary>
        ///     A record was found for the address and may be decoded.
        /// </summary>
        Found,

        /// <summary>
        ///     The database contains no record for the address.
        /// </summary>
        NotFound,

        /// <summary>
        ///     Resolving the address failed because the database is corrupt.
        ///     The captured exception is available on
        ///     <see cref="BatchLookup.Exception" />.
        /// </summary>
        Faulted,

        /// <summary>
        ///     The address was not processed because cancellation was
        ///     requested before its position in the input was reached.
        /// </summary>
        Cancelled,
    }

    /// <summary>
    ///     Options for <see cref="Reader.FindBatch{TResult}" />.
    /// </summary>
    public sealed class BatchLookupOptions
    {
        /// <summary>
        ///     The maximum number of addresses whose search-tree resolution
        ///     may run in parallel. The default is 1 (fully sequential).
        ///     Parallelism never changes the result order, the first error
        ///     surfaced, or the cancellation position; those are always
        ///     determined by the input order.
        /// </summary>
        public int MaxDegreeOfParallelism { get; set; } = 1;
    }

    /// <summary>
    ///     Projects a single batch lookup outcome into a caller-defined value.
    /// </summary>
    /// <typeparam name="TResult">The projected result type.</typeparam>
    /// <param name="lookup">
    ///     A controlled view of the lookup outcome. The view is only valid
    ///     for the duration of this callback; it cannot be stored or
    ///     captured for later use.
    /// </param>
    /// <returns>The value stored in the results for this address.</returns>
    public delegate TResult BatchProjection<out TResult>(BatchLookup lookup);

    /// <summary>
    ///     A controlled, short-lived view of the outcome of looking up one
    ///     address in a batch. The view is only valid inside the
    ///     <see cref="BatchProjection{TResult}" /> callback it was passed to:
    ///     as a <c>ref struct</c> it cannot be boxed, captured, or stored, so
    ///     it can never outlive the callback or observe the reader after
    ///     disposal. Decoding through the view fully materializes the record;
    ///     the returned object never references the memory-mapped database.
    /// </summary>
    public ref struct BatchLookup
    {
        private readonly Reader _reader;
        private readonly long _pointer;
        private readonly int _prefixLength;
        private readonly Exception? _exception;
        private Network? _network;

        internal BatchLookup(
            Reader reader,
            IPAddress address,
            BatchLookupStatus status,
            long pointer,
            int prefixLength,
            bool isIPv4InIPv6,
            Exception? exception)
        {
            _reader = reader;
            Address = address;
            Status = status;
            _pointer = pointer;
            _prefixLength = prefixLength;
            IsIPv4InIPv6 = isIPv4InIPv6;
            _exception = exception;
            _network = null;
        }

        /// <summary>
        ///     The address this lookup was performed for.
        /// </summary>
        public IPAddress Address { get; }

        /// <summary>
        ///     The outcome of the lookup for <see cref="Address" />.
        /// </summary>
        public BatchLookupStatus Status { get; }

        /// <summary>
        ///     Whether an IPv4 address was resolved through the IPv4-mapped
        ///     subtree of an IPv6 database.
        /// </summary>
        public bool IsIPv4InIPv6 { get; }

        /// <summary>
        ///     The network containing the address, or <c>null</c> when the
        ///     status is not <see cref="BatchLookupStatus.Found" />. The
        ///     instance is created lazily so that projections which do not
        ///     need it pay no allocation.
        /// </summary>
        public Network? Network
        {
            get
            {
                if (Status != BatchLookupStatus.Found)
                {
                    return null;
                }

                return _network ??= new Network(Address, _prefixLength);
            }
        }

        /// <summary>
        ///     The exception captured when the status is
        ///     <see cref="BatchLookupStatus.Faulted" />; otherwise <c>null</c>.
        /// </summary>
        public Exception? Exception => _exception;

        /// <summary>
        ///     Decodes the database record for this address. The returned
        ///     object is fully materialized and does not reference the
        ///     memory-mapped database, so it remains valid after the batch
        ///     completes and after the reader is disposed.
        /// </summary>
        /// <typeparam name="T">The type to decode the record as.</typeparam>
        /// <param name="injectables">Values to inject during deserialization.</param>
        /// <returns>The decoded record.</returns>
        /// <exception cref="InvalidOperationException">
        ///     The lookup status is not <see cref="BatchLookupStatus.Found" />.
        /// </exception>
        /// <exception cref="InvalidDatabaseException">
        ///     The record in the data section is corrupt. This is the same
        ///     exception a single <see cref="Reader.Find{T}(IPAddress, InjectableValues)" /> would throw
        ///     for the address.
        /// </exception>
        public T Decode<T>(InjectableValues? injectables = null) where T : class
        {
            if (Status == BatchLookupStatus.Faulted)
            {
                ExceptionDispatchInfo.Capture(_exception!).Throw();
            }

            if (Status != BatchLookupStatus.Found)
            {
                throw new InvalidOperationException(
                    $"Cannot decode a batch lookup with status {Status}.");
            }

            return _reader.DecodeBatchPointer<T>(_pointer, injectables, Network);
        }
    }
}
