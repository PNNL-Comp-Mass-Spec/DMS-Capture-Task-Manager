using System;
using System.Collections.Generic;
using System.Net;
using System.Text.RegularExpressions;
using Jayrock.Json.Conversion;
using PRISM;

namespace Pacifica.Core
{
    /// <summary>
    /// Examine the status of a given ingest job
    /// </summary>
    /// <remarks>
    /// First call GetIngestStatus then call IngestStepCompleted.
    /// This allows for just one web request, but the ability to examine the status of multiple steps
    /// </remarks>
    public class MyEMSLStatusCheck : clsEventNotifier
    {
        public const string PERMISSIONS_ERROR = "Permissions error:";

        private readonly Configuration mPacificaConfig;

        /// <summary>
        /// Error message
        /// </summary>
        public string ErrorMessage
        {
            get;
            set;
        }

        /// <summary>
        /// Constructor
        /// </summary>
        public MyEMSLStatusCheck()
        {
            ErrorMessage = string.Empty;
            mPacificaConfig = new Configuration();

            EasyHttp.MyEMSLOffline += EasyHttp_MyEMSLOffline;
        }

        /// <summary>
        /// Check whether a file exists in MyEMSL
        /// </summary>
        /// <param name="fileInfo">File info object</param>
        /// <returns>True if found, otherwise false</returns>
        /// <remarks>Searches using Sha1HashHex, so could match a file in a different location than the specific path tracked by fileInfo</remarks>
        public bool DoesFileExistInMyEMSL(FileInfoObject fileInfo)
        {
            var fileSHA1HashSum = fileInfo.Sha1HashHex;

            // Example URL
            // https://metadata.my.emsl.pnl.gov/files?hashsum=7b05677da8a6a5c8d033e56dd36ab5445ae44860
            var metadataURL = mPacificaConfig.MetadataServerUri + "/files?hashsum=" + fileSHA1HashSum;

            if (!ValidateCertFile("DoesFileExistInMyEMSL", out _))
            {
                return false;
            }

            var fileListJSON = EasyHttp.Send(mPacificaConfig, metadataURL, out var responseStatusCode);

            // Example response for just one file (hashsum=0a7bcbcf4085abc41bdbd98724f3e5c567726c56)
            // [{"mimetype": "application/octet-stream", "updated": "2017-07-02T23:54:53", "name": "QC_Mam_16_01_125ng_HCD-3_30Jun17_Frodo_REP-17-06-01_msgfplus_syn_ProteinMods.txt", "created": "2017-07-02T23:54:53", "deleted": null, "size": 899907, "hashsum": "0a7bcbcf4085abc41bdbd98724f3e5c567726c56", "hashtype": "sha1", "subdir": "MSG201707021504_Auto1467864", "mtime": "2017-07-02T23:49:14", "_id": 15578789, "encoding": "UTF8", "transaction_id": 1302996, "ctime": "2017-07-02T23:53:28"}]

            // Example response for multiple files (hashsum=627ad3a8a1eaad358e0c89f8e5b7db1473f33278):
            // [{"mimetype": "None", "updated": "2017-06-30T03:08:36", "name": "MSGFDB_PartArgC_MetOx_StatCysAlk_20ppmParTol_ModDefs.txt", "created": "2017-06-30T03:08:36", "deleted": null, "size": 52, "hashsum": "627ad3a8a1eaad358e0c89f8e5b7db1473f33278", "hashtype": "sha1", "subdir": "MSG201405141726_Auto1058369", "mtime": "2014-05-14T18:00:53", "_id": 3694295, "encoding": "UTF-8", "transaction_id": 443104, "ctime": "2014-05-14T18:01:08"}, {"mimetype": "None", "updated": "2017-06-30T03:23:14", "name": "MSGFDB_PartAspN_MetOx_StatCysAlk_20ppmParTol_ModDefs.txt", "created": "2017-06-30T03:23:14", "deleted": null, "size": 52, "hashsum": "627ad3a8a1eaad358e0c89f8e5b7db1473f33278", "hashtype": "sha1", "subdir": "MSG201405141729_Auto1058370", "mtime": "2014-06-03T13:43:05", "_id": 3841932, "encoding": "UTF-8", "transaction_id": 457902, "ctime": "2014-06-03T13:43:09"}]

            var jsa = (Jayrock.Json.JsonArray)JsonConvert.Import(fileListJSON);
            var fileList = Utilities.JsonArrayToDictionaryList(jsa);

            if (responseStatusCode.ToString() == "200" && fileListJSON != "[]" && fileListJSON.Contains(fileSHA1HashSum))
            {
                return true;
            }
            return false;
        }

        /// <summary>
        /// Obtain the status returned by the given MyEMSL status page
        /// </summary>
        /// <param name="statusURI">
        /// URI to examine, e.g. https://ingestdms.my.emsl.pnl.gov/get_state?job_id=1300782
        /// </param>
        /// <param name="percentComplete">Output: ingest process percent complete (value between 0 and 100)</param>
        /// <param name="lookupError">Output: true if an error occurs</param>
        /// <param name="errorMessage">Output: error message if lookupError is true</param>
        /// <returns>Status dictionary (empty dictionary if an error)</returns>
        public Dictionary<string, object> GetIngestStatus(
            string statusURI,
            out int percentComplete,
            out bool lookupError,
            out string errorMessage)
        {

            lookupError = false;

            if (!ValidateCertFile("GetIngestStatus", out errorMessage))
            {
                percentComplete = 0;
                lookupError = true;
                return new Dictionary<string, object>();
            }

            // The following Callback allows us to access the MyEMSL server even if the certificate is expired or untrusted
            // For more info, see comments in Upload.StartUpload()
            if (ServicePointManager.ServerCertificateValidationCallback == null)
                ServicePointManager.ServerCertificateValidationCallback += Utilities.ValidateRemoteCertificate;

            OnDebugEvent("Contacting " + statusURI);

            var statusResult = EasyHttp.Send(mPacificaConfig, statusURI, out _);

            OnDebugEvent("Result received: " + statusResult);

            // Example contents of statusResult
            // (as returned by https://ingestdms.my.emsl.pnl.gov/get_state?job_id=123456)
            // {"task_percent": "0.00000", "state": "OK",     "task": "UPLOADING",         "job_id": 104}      (starting)
            // {"task_percent": "0.00000", "state": "FAILED", "task": "Policy Validation", "job_id": 104}      (error)
            // {"task_percent": "0.00000", "state": "FAILED", "task": "ingest metadata",   "job_id": 1300782}  (error)
            // {"task_percent": "0.00000", "state": "FAILED", "task": "ingest files",      "job_id": 1301499}  (error)
            // {"task_percent": "100.00000", "state": "OK", "task": "ingest metadata",     "job_id": 1300004}  (complete)
            // {"task_percent": "0.00000", "updated": "2017-07-06 22:00:49", "task": "ingest files", "job_id": 1303430, "created": "2017-07-06 22:00:51", "exception": "", "state": "OK"}

            var statusJSON = Utilities.JsonToObject(statusResult);

            var state = Utilities.GetDictionaryValue(statusJSON, "state").ToLower();

            var task = Utilities.GetDictionaryValue(statusJSON, "task");

            var exception = Utilities.GetDictionaryValue(statusJSON, "exception");

            var percentCompleteText = Utilities.GetDictionaryValue(statusJSON, "task_percent");

            if (float.TryParse(percentCompleteText, out var percentCompleteFloat))
            {
                percentComplete = (int)percentCompleteFloat;
            }
            else
            {
                percentComplete = 0;
            }

            switch (state)
            {
                case "ok":
                    if (string.IsNullOrWhiteSpace(exception))
                    {
                        OnDebugEvent("Archive state is OK for " + statusURI);
                    }
                    else
                    {
                        errorMessage = "Upload state is OK, but an exception was reported for task \"" + task + "\"" +
                                       "; exception \"" + exception + "\"";

                        OnErrorEvent(errorMessage + "; see " + statusURI);
                    }
                    break;

                case "failed":
                    errorMessage = "Upload failed, task \"" + task + "\"";
                    if (string.IsNullOrWhiteSpace(exception))
                    {
                        OnErrorEvent(string.Format("{0}; see {1}", errorMessage, statusURI));
                    }
                    else
                    {
                        OnErrorEvent(string.Format("{0}; exception \"{1}\"; see {2}", errorMessage, exception, statusURI));

                        if (exception.IndexOf("ConnectionTimeout", StringComparison.OrdinalIgnoreCase) >= 0)
                        {
                            errorMessage += "; ConnectionTimeout exception";
                        }
                        else
                        {
                            // Unrecognized exception; include the first 75 characters
                            if (exception.Length < 80)
                                errorMessage += "; exception " + exception;
                            else
                                errorMessage += "; exception " + exception.Substring(0, 75) + " ...";
                        }
                    }

                    break;

                default:
                    if (state.Contains("error"))
                    {
                        OnErrorEvent("Status server is offline or having issues; cannot check " + statusURI);
                    }
                    else
                    {
                        OnErrorEvent("Unrecognized state " + state + " for " + statusURI);
                    }
                    break;
            }

            return statusJSON;
        }

        /// <summary>
        /// Extract the StatusNum (StatusID) from a status URI
        /// </summary>
        /// <param name="statusURI"></param>
        /// <returns>The status number, or 0 if an error</returns>
        public static int GetStatusNumFromURI(string statusURI)
        {
            // Check for a match to a URI of the form
            // https://ingestdms.my.emsl.pnl.gov/get_state?job_id=1302995

            var reGetstatusNum = new Regex(@"job_id=(\d+)");

            var match = reGetstatusNum.Match(statusURI);
            if (match.Success)
            {
                var statusNum = int.Parse(match.Groups[1].Value);

                if (statusNum <= 0)
                    throw new Exception("Status ID is 0 in StatusURI: " + statusURI);

                return statusNum;
            }

            // Check for a match to a URI of the form
            // https://a4.my.emsl.pnl.gov/myemsl/cgi-bin/status/2381528/xml
            var reLegacyStatusNum = new Regex(@"(\d+)/xml", RegexOptions.IgnoreCase);
            var legacyMatch = reLegacyStatusNum.Match(statusURI);
            if (!legacyMatch.Success)
                throw new Exception("Could not find Status ID in StatusURI: " + statusURI);

            var legacyStatusNum = int.Parse(legacyMatch.Groups[1].Value);

            if (legacyStatusNum <= 0)
                throw new Exception("Status ID is 0 in StatusURI: " + statusURI);

            return legacyStatusNum;
        }

        /// <summary>
        /// Percent complete (value between 0 and 100)
        /// </summary>
        /// <param name="percentComplete"></param>
        /// <returns>Number of steps completed</returns>
        /// <remarks>Reports 7 when percentComplete is 100</remarks>
        public byte IngestStepCompletionCount(int percentComplete)
        {
            // Convert the percent complete value to a number between 0 and 7
            // since historically there were 7 steps to the ingest process:
            // 1. Submitted        .tar file submitted
            // 2. Received         .tar file received
            // 3. Processing       .tar file being processed
            // 4. Verified         .tar file contents validated
            // 5. Stored           .tar file contents copied to Aurora
            // 6. Available        Available in Elastic Search
            // 7. Archived         Data copied to tape

            var stepsCompleted = (byte)(Math.Round(7 * (percentComplete / 100.0)));

            return stepsCompleted;
        }

        /// <summary>
        /// Report true if the error message contains a critical error
        /// </summary>
        /// <param name="errorMessage"></param>
        /// <returns></returns>
        public bool IsCriticalError(string errorMessage)
        {
            if (errorMessage.StartsWith("error submitting ingest job"))
            {
                return true;
            }

            return false;
        }

        /// <summary>
        /// Validate that the MyEMSL certificate file exists
        /// </summary>
        /// <param name="callingMethod">Calling method</param>
        /// <param name="errorMessage">Output: error message</param>
        /// <returns>True if the cert file is found, otherwise false</returns>
        private bool ValidateCertFile(string callingMethod, out string errorMessage)
        {
            var certificateFilePath = EasyHttp.ResolveCertFile(mPacificaConfig, callingMethod, out errorMessage);

            if (!string.IsNullOrWhiteSpace(certificateFilePath))
                return true;

            OnErrorEvent(errorMessage);
            return false;
        }


        private void EasyHttp_MyEMSLOffline(object sender, MessageEventArgs e)
        {
            OnWarningEvent("MyEMSL is offline; unable to retrieve data: " + e.Message);
        }

    }
}
