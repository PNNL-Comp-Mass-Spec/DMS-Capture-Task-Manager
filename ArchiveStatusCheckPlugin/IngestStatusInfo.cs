
namespace ArchiveStatusCheckPlugin
{
    public class IngestStatusInfo
    {
        // Ignore Spelling: EUS

        /// <summary>
        /// Status Number, e.g. 3257122
        /// </summary>
        public int StatusNum { get; }

        /// <summary>
        /// Example status URI for 2017: https://ingestdms.my.emsl.pnl.gov/get_state?job_id=1302995
        /// Example legacy Status URI:   https://ingest.my.emsl.pnl.gov/myemsl/cgi-bin/status/3257122/xml
        /// </summary>
        public string StatusURI { get; set; }

        /// <summary>
        /// Subdirectory name
        /// </summary>
        /// <remarks>If empty, all files below the dataset are uploaded</remarks>
        public string Subdirectory { get; set; }

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
        /// EUS Project ID, e.g. 46206
        /// </summary>
        public string EUS_ProjectID { get; set; }

        /// <summary>
        /// EUS Uploader ID (typically instrument operator), e.g. 41133
        /// </summary>
        public int EUS_UploaderID { get; set; }

        /// <summary>
        /// Error code, prior to the most recent check
        /// </summary>
        public int ExistingErrorCode { get; set; }

        /// <summary>
        /// Constructor that takes Status_Num and Status_URI
        /// </summary>
        public IngestStatusInfo(int statusNum, string statusURI)
        {
            StatusNum = statusNum;
            StatusURI = statusURI;
            Subdirectory = string.Empty;

            IngestStepsCompletedOld = 0;
            IngestStepsCompletedNew = 0;

            EUS_InstrumentID = 0;
            EUS_ProjectID = string.Empty;
            EUS_UploaderID = 0;
        }

        public override string ToString()
        {
            return StatusURI;
        }
    }
}
