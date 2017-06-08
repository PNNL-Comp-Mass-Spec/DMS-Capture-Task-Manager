
namespace ArchiveStatusCheckPlugin
{
    public class clsIngestStatusInfo
    {
        /// <summary>
        /// Status Number, e.g. 3257122
        /// </summary>
        public int StatusNum { get; }

        /// <summary>
        /// Status URI, e.g. https://ingest.my.emsl.pnl.gov/myemsl/cgi-bin/status/3257122/xml
        /// </summary>
        public string StatusURI { get; set; }

        /// <summary>
        /// Subfolder name
        /// </summary>
        /// <remarks>If empty, all files below the dataset are uploaded</remarks>
        public string Subfolder { get; set; }

        /// <summary>
        /// TransactionID for the upload bundle
        /// </summary>
        /// <remarks>
        /// To see files uploaded by TransactionID, use https://status.my.emsl.pnl.gov/status/view/t/transactionID
        /// For example: https://status.my.emsl.pnl.gov/status/view/t/1257122
        /// </remarks>
        public int TransactionId { get; set; }

        /// <summary>
        /// Number of ingest steps completed, before the most recent check
        /// </summary>
        public byte IngestStepsCompletedOld { get; set; }

        /// <summary>
        /// Number of ingest steps completed, after the most recent check
        /// </summary>
        public byte IngestStepsCompletedNew { get; set; }

        /// <summary>
        /// EUS Instrument ID, e.g. 34127
        /// </summary>
        public int EUS_InstrumentID { get; set; }

        /// <summary>
        /// EUS Proposal ID, e.g. 46206
        /// </summary>
        public string EUS_ProposalID { get; set; }

        /// <summary>
        /// EUS Uploader ID (typically instrument operator), e.g. 41133
        /// </summary>
        public int EUS_UploaderID { get; set; }

        /// <summary>
        /// Error code, prior to the most recen tcheck
        /// </summary>
        public int ExistingErrorCode { get; set; }

        /// <summary>
        /// Constructor that takes statusNum
        /// </summary>
        /// <remarks>Calls the other constructor with an empty string for statusURI</remarks>
        public clsIngestStatusInfo(int statusNum) : this(statusNum, string.Empty)
        {
        }

        /// <summary>
        /// Constructor that takes statusNum and statusURI
        /// </summary>
        public clsIngestStatusInfo(int statusNum, string statusURI)
        {
            StatusNum = statusNum;
            StatusURI = statusURI;
            Subfolder = string.Empty;
            TransactionId = 0;

            IngestStepsCompletedOld = 0;
            IngestStepsCompletedNew = 0;

            EUS_InstrumentID = 0;
            EUS_ProposalID = string.Empty;
            EUS_UploaderID = 0;

        }
    }
}
