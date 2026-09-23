#region

#endregion

namespace MaxMind.Db
{
    /// <summary>
    ///     The outcome of a single address lookup within a batch.
    /// </summary>
    public enum BatchLookupStatus
    {
        /// <summary>
        ///     The database contains no record for the address. This is a
        ///     normal result, not an error: the batch continues with the
        ///     remaining items.
        /// </summary>
        NotFound = 0,

        /// <summary>
        ///     A record was found for the address and may be decoded through
        ///     <see cref="BatchLookupView.Decode{T}"/>.
        /// </summary>
        Found = 1,
    }
}
