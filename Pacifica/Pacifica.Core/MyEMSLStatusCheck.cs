using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text.RegularExpressions;
using System.Xml;
using Jayrock.Json.Conversion;

namespace Pacifica.Core
{
    /// <summary>
    /// Examine the status of a given ingest job
    /// </summary>
    /// <remarks>
    /// First call GetIngestStatus then call IngestStepCompleted.
    /// This allows for just one web request, but the ability to examine the status of multiple steps
    /// </remarks>
    public class MyEMSLStatusCheck
    {
        public const string PERMISSIONS_ERROR = "Permissions error:";

        private readonly Configuration mPacificaConfig;

        /// <summary>
        /// Upload status
        /// </summary>
        public enum StatusStep
        {
            Submitted = 0,      // .tar file submitted
            Received = 1,       // .tar file received
            Processing = 2,     // .tar file being processed
            Verified = 3,       // .tar file contents validated
            Stored = 4,         // .tar file contents copied to Aurora
            Available = 5,      // Available in Elastic Search
            Archived = 6        // Sha-1 hash values of files in Aurora validated against expected hash values
        }

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
        }

        [Obsolete("Unused")]
        private bool CheckMetadataValidity(string metadataObjectJSON)
        {
            var policyURL = mPacificaConfig.PolicyServerUri + "/ingest/";
            HttpStatusCode responseStatusCode;
            var success = EasyHttp.Send(mPacificaConfig, policyURL, out responseStatusCode, metadataObjectJSON, EasyHttp.HttpMethod.Post);
            if (responseStatusCode.ToString() == "200" && success.ToLower() == "true")
            {
                return true;
            }
            return false;
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
            var metadataURL = mPacificaConfig.MetadataServerUri + "/files?hashsum=" + fileSHA1HashSum;

            HttpStatusCode responseStatusCode;

            var fileListJSON = EasyHttp.Send(mPacificaConfig, metadataURL, out responseStatusCode);
            var jsa = (Jayrock.Json.JsonArray)JsonConvert.Import(fileListJSON);
            var fileList = Utilities.JsonArrayToDictionaryList(jsa);

            if (responseStatusCode.ToString() == "200" && fileListJSON != "[]" && fileListJSON.Contains(fileSHA1HashSum))
            {
                return true;
            }
            return false;
        }

        /// <summary>
        /// Obtain the XML returned by the given MyEMSL status page
        /// </summary>
        /// <param name="statusURI">URI to examine</param>
        /// <param name="cookieJar">Cookies</param>
        /// <param name="lookupError">Output: true if an error occurs</param>
        /// <param name="errorMessage">Output: error message if lookupError is true</param>
        /// <returns>Status message, in XML format; empty string if an error</returns>
        [Obsolete("Use the version that uses svc-dms.pfx instead of cookies")]
        public string GetIngestStatus(
            string statusURI,
            CookieContainer cookieJar,
            out bool lookupError,
            out string errorMessage)
        {
            var errorMessageKeywords = new List<string>
            {   "exceptions.",
                "exception"
            };

            var errorMessageTerminators = new List<string>
            {   "traceback",
                "status=",
                "/>",
                @"\"
            };

            lookupError = false;
            errorMessage = string.Empty;

            // The following Callback allows us to access the MyEMSL server even if the certificate is expired or untrusted
            // For more info, see comments in Upload.StartUpload()
            if (ServicePointManager.ServerCertificateValidationCallback == null)
                ServicePointManager.ServerCertificateValidationCallback += Utilities.ValidateRemoteCertificate;

            const int timeoutSeconds = 30;
            HttpStatusCode responseStatusCode;

            var xmlServerResponse = EasyHttp.Send(mPacificaConfig, statusURI, out responseStatusCode, timeoutSeconds);

            // Look for an exception in the response
            // Example response with an error:
            //
            //  <transaction id='1025302' />
            //  <step id='0' message='&lt;type 'exceptions.IOError'&gt;
            //   [Errno 5] Input/output error
            //   &lt;traceback object at 0x7fcb9e646170&gt;
            //   Traceback%20%28most%20recent%20call%20last%29%3A%0A%20%20File%20%22%2Fusr%2Flib%2Fpython2.6%2Fsite-packages%2Fmyemsl%2Fcatchall.py ...
            //   ' status='ERROR' />

            var exceptionIndex = -1;
            var keywordLength = 0;

            foreach (var exceptionKeyword in errorMessageKeywords)
            {
                exceptionIndex = xmlServerResponse.IndexOf(exceptionKeyword, StringComparison.InvariantCultureIgnoreCase);
                if (exceptionIndex > 0)
                {
                    keywordLength = exceptionKeyword.Length;
                    break;
                }
            }

            if (exceptionIndex < 0)
            {
                return xmlServerResponse;
            }

            var message = xmlServerResponse.Substring(exceptionIndex + keywordLength);

            foreach (var terminator in errorMessageTerminators)
            {
                var charIndex = message.IndexOf(terminator, StringComparison.InvariantCultureIgnoreCase);
                if (charIndex > 0)
                {
                    message = message.Substring(0, charIndex - 1)
                                .Replace("\n", "; ")
                                .Replace("&lt;", string.Empty)
                                .Replace("&lt", string.Empty)
                                .Replace("&gt;", "; ")
                                .Replace("&gt", "; ")
                                .Replace("; ;", ";")
                                .Replace("';", ";")
                                .TrimEnd(' ', ';');
                    break;
                }
            }

            errorMessage = "Exception: " + message;
            lookupError = true;

            return string.Empty;
        }

        /// <summary>
        /// Obtain the status returned by the given MyEMSL status page
        /// </summary>
        /// <param name="statusURI">URI to examine</param>
        /// <param name="lookupError">Output: true if an error occurs</param>
        /// <param name="errorMessage">Output: error message if lookupError is true</param>
        /// <returns>Status dictionary; empty dictionary</returns>
        public Dictionary<string, object> GetIngestStatus(
            string statusURI,
            out bool lookupError,
            out string errorMessage)
        {

            lookupError = false;
            errorMessage = string.Empty;

            if (!File.Exists(Configuration.CLIENT_CERT_FILEPATH))
            {
                errorMessage = "Authentication failure; cert file not found at " + Configuration.CLIENT_CERT_FILEPATH;
                ReportError("GetIngestStatus", errorMessage);
                lookupError = true;
                return new Dictionary<string, object>();
            }

            // The following Callback allows us to access the MyEMSL server even if the certificate is expired or untrusted
            // For more info, see comments in Upload.StartUpload()
            if (ServicePointManager.ServerCertificateValidationCallback == null)
                ServicePointManager.ServerCertificateValidationCallback += Utilities.ValidateRemoteCertificate;

            var statusResult = EasyHttp.Send(mPacificaConfig, statusURI, out HttpStatusCode responseStatusCode);

            var statusJSON = Utilities.JsonToObject(statusResult);

            var state = Utilities.GetDictionaryValue(statusJSON, "state").ToLower();

            // ToDo: confirm that task_percent reports 100%, e.g.
            // {"task_percent": "100.00000", "state": "OK", "task": "ingest metadata", "job_id": 1300004}     (complete since 100%)

            if (state == "ok")
            {
                OnDebugMessage(new MessageEventArgs("GetIngestStatus", "Archive state is OK for " + statusURI));
            }
            else if (state == "failed")
            {
                ReportError("GetIngestStatus", "Upload failed ");
            }
            else if (state.Contains("error"))
            {
                ReportError("GetIngestStatus", "Status server is offline or having issues");
            }
            else
            {
                ReportError("GetIngestStatus", "Unrecognized state: " + state);
            }

            return statusJSON;
        }

        private string GetStepDescription(StatusStep step)
        {
            return string.Format("{0} ({1})", (int)step, step);
        }

        protected bool HasExceptions(string xmlServerResponse, bool reportError, out string errorMessage)
        {
            const string EXCEPTION_TEXT = @"message='exceptions.";
            errorMessage = string.Empty;

            var exceptionIndex = xmlServerResponse.IndexOf(EXCEPTION_TEXT, StringComparison.Ordinal);
            if (exceptionIndex <= 0)
            {
                return false;
            }

            var exceptionMessage = xmlServerResponse.Substring(exceptionIndex + EXCEPTION_TEXT.Length);
            var charIndex = exceptionMessage.IndexOf("traceback", StringComparison.Ordinal);
            if (charIndex > 0)
                exceptionMessage = exceptionMessage.Substring(0, charIndex - 1).Replace("\n", "; ").Replace("&lt", string.Empty);
            else
            {
                charIndex = exceptionMessage.IndexOf('\'', 5);
                if (charIndex > 0)
                    exceptionMessage = exceptionMessage.Substring(0, charIndex - 1).Replace("\n", "; ");
            }

            if (reportError)
            {
                errorMessage = "Exception: " + exceptionMessage;
                ReportError("HasExceptions", errorMessage);
            }

            return true;
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
        /// Examines the status of each step in xmlServerResponse to see if any of them contain status Error
        /// </summary>
        /// <param name="xmlServerResponse"></param>
        /// <param name="errorMessage">Output: error messge</param>
        /// <returns>True if an error, false if no errors</returns>
        [Obsolete("Use the version that parses the JSON-based dictionary server response")]
        public bool HasStepError(string xmlServerResponse, out string errorMessage)
        {
            errorMessage = string.Empty;

            var stepNumbers = new List<StatusStep>();

            var xmlDoc = new XmlDocument();
            xmlDoc.LoadXml(xmlServerResponse);

            // Find all step elements that contain an id attribute
            // See function IngestStepCompleted for Example XML
            var stepNodes = xmlDoc.SelectNodes("//step[@id]");

            if (stepNodes != null)
            {
                foreach (XmlNode stepNode in stepNodes)
                {
                    if (stepNode.Attributes == null)
                    {
                        continue;
                    }

                    var stepID = stepNode.Attributes.GetNamedItem("id");
                    int stepNumber;
                    if (!int.TryParse(stepID.Value, out stepNumber))
                    {
                        continue;
                    }

                    if (Enum.IsDefined(typeof(StatusStep), stepNumber))
                    {
                        stepNumbers.Add((StatusStep)stepNumber);
                    }
                }
            }

            foreach (var stepNum in stepNumbers)
            {
                string statusMessage;

                if (IngestStepCompleted(xmlServerResponse, stepNum, out statusMessage, out errorMessage))
                {
                    continue;
                }

                if (!string.IsNullOrEmpty(errorMessage))
                    return true;
            }

            return false;

        }

        /// <summary>
        /// Examines the status of each step in xmlServerResponse to see if any of them contain status Error
        /// </summary>
        /// <param name="serverResponse"></param>
        /// <param name="errorMessage">Output: error messge</param>
        /// <returns>True if an error, false if no errors</returns>
        public bool HasStepError(Dictionary<string, object> serverResponse, out string errorMessage)
        {
            errorMessage = string.Empty;

            // ToDo: write this code

            //var stepNumbers = new List<StatusStep>();

            //foreach (var stepNum in stepNumbers)
            //{
            //    string statusMessage;

            //    if (IngestStepCompleted(xmlServerResponse, stepNum, out statusMessage, out errorMessage))
            //    {
            //        continue;
            //    }

            //    if (!string.IsNullOrEmpty(errorMessage))
            //        return true;
            //}

            return false;

        }

        /// <summary>
        /// This function examines the xml returned by a MyEMSL status page to determine whether or not the step succeeded
        /// </summary>
        /// <param name="xmlServerResponse"></param>
        /// <param name="stepNum">Step number whose status should be examined</param>
        /// <param name="statusMessage">Output parameter: status message for step stepNum</param>
        /// <param name="errorMessage">Output parameter: status message for step stepNum</param>
        /// <returns>True if step stepNum has successfully completed</returns>
        [Obsolete("This method is for the legacy XML-based ingest status")]
        public bool IngestStepCompleted(
            string xmlServerResponse,
            StatusStep stepNum,
            out string statusMessage,
            out string errorMessage)
        {
            const string UPLOAD_PERMISSION_ERROR = "do not have upload permissions";

            statusMessage = string.Empty;

            // First look for exceptions
            if (HasExceptions(xmlServerResponse, true, out errorMessage))
            {
                // Exceptions are present; step is not complete
                statusMessage = "Ingest status reports exceptions";
                return false;
            }

            var xmlDoc = new XmlDocument();
            xmlDoc.LoadXml(xmlServerResponse);

            // Example XML:
            //
            // <?xml version="1.0"?>
            // <myemsl>
            //  <status username='70000'>
            //      <transaction id='111177' />
            //      <step id='0' message='completed' status='SUCCESS' />
            //      <step id='1' message='completed' status='SUCCESS' />
            //      <step id='2' message='completed' status='SUCCESS' />
            //      <step id='3' message='completed' status='SUCCESS' />
            //      <step id='4' message='completed' status='SUCCESS' />
            //      <step id='5' message='completed' status='SUCCESS' />
            //      <step id='6' message='verified' status='SUCCESS' />
            //  </status>
            // </myemsl>
            //
            // Step IDs correspond to:
            // 0: Submitted
            // 1: Received
            // 2: Processing
            // 3: Verified
            // 4: Stored
            // 5: Available   (status will be "ERROR" if user doesn't have upload permissions for a proposal;
            //                 for example https://a4.my.emsl.pnl.gov/myemsl/cgi-bin/status/1042281/xml shows message
            //                 "You(47943) do not have upload permissions to proposal 17797"
            //                 for user svc-dms on May 3, 2012)
            //                 And https://ingest.my.emsl.pnl.gov/myemsl/cgi-bin/status/2919668/xml shows message
            //                 "Invalid Permissions" on January 4, 2016
            // 6: Archived    (status will be "UNKNOWN" if not yet verified)

            var transactionElement = xmlDoc.SelectSingleNode("//transaction");
            if (transactionElement?.Attributes == null)
            {
                errorMessage = "transaction element not found in the Status XML";
                ReportError("IngestStepCompleted", errorMessage);
                statusMessage = errorMessage;
                return false;
            }

            var query = string.Format("//step[@id='{0}']", (int)stepNum);
            var statusElement = xmlDoc.SelectSingleNode(query);

            if (statusElement?.Attributes == null)
            {
                errorMessage = string.Format("Match not found for step {0} in the Status XML", GetStepDescription(stepNum));
                ReportError("IngestStepCompleted", errorMessage);
                statusMessage = string.Format("Match not found for step {0}", GetStepDescription(stepNum));
                return false;
            }

            var message = statusElement.Attributes["message"].Value;
            var status = statusElement.Attributes["status"].Value;

            if (string.IsNullOrEmpty(message))
            {
                errorMessage = "message attribute in the Status XML is empty for step " + GetStepDescription(stepNum);
                statusMessage = "Status XML error";
                return false;
            }

            if (string.IsNullOrEmpty(status))
            {
                errorMessage = "status attribute in the Status XML is empty for step " + GetStepDescription(stepNum);
                statusMessage = "Status XML error";
                return false;
            }

            if (status.ToLower() == "error")
            {

                if (message.Contains(UPLOAD_PERMISSION_ERROR))
                {
                    errorMessage = PERMISSIONS_ERROR + " " + message;
                    statusMessage = PERMISSIONS_ERROR;
                }
                else
                {
                    errorMessage = message;
                    statusMessage = "Ingest error";
                }

                return false;

            }

            if (status.ToLower() == "success")
            {
                if (message.ToLower() == "completed")
                {
                    statusMessage = string.Format("Step {0} is complete", GetStepDescription(stepNum));
                    return true;
                }

                if (message.ToLower() == "verified")
                {
                    statusMessage = string.Format("Data is verified, step {0}", GetStepDescription(stepNum));
                    return true;
                }

                return false;
            }

            if (status.ToLower() == "unknown")
            {
                // Step is not yet complete
                statusMessage = string.Format("Waiting on step {0}", GetStepDescription(stepNum));
                return false;
            }

            // Status is not empty, error, success, or unknown
            // Unrecognized state

            errorMessage = string.Format("Unrecognized status state for step {0}: {1}", GetStepDescription(stepNum), status);
            statusMessage = "Unrecognized status";
            return false;

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
            // since historically there were 7 steps to the ingest process

            var stepsCompleted = (byte)(Math.Round(7 * (percentComplete / 100.0)));

            return stepsCompleted;
        }

        [Obsolete("This method is for the legacy XML-based ingest status")]
        public byte IngestStepCompletionCount(string xmlServerResponse)
        {
            string errorMessage;

            // First look for exceptions
            if (string.IsNullOrWhiteSpace(xmlServerResponse) || HasExceptions(xmlServerResponse, false, out errorMessage))
            {
                // Exceptions are present; report 0 steps complete
                return 0;
            }

            var xmlDoc = new XmlDocument();
            xmlDoc.LoadXml(xmlServerResponse);

            // Find all step elements that contain an id attribute
            // See function IngestStepCompleted for Example XML
            var stepNodes = xmlDoc.SelectNodes("//step[@id]");

            if (stepNodes == null || stepNodes.Count == 0)
            {
                // Did not find any step nodes in the Status XML
                return 0;
            }

            byte ingestStepsCompleted = 0;

            foreach (XmlNode stepNode in stepNodes)
            {
                if (stepNode.Attributes == null)
                    continue;

                var message = stepNode.Attributes["message"].Value;
                var status = stepNode.Attributes["status"].Value;

                if (string.IsNullOrEmpty(message))
                {
                    // Message attribute in the Status XML is empty
                    continue;
                }

                if (string.IsNullOrEmpty(status))
                {
                    // Status attribute in the Status XML is empty
                    continue;
                }

                if (status.ToLower() == "success")
                {
                    message = message.ToLower();

                    if (message == "completed" || message == "verified")
                    {
                        ingestStepsCompleted++;
                    }
                }
            }

            return ingestStepsCompleted;

        }

        /// <summary>
        /// Determine the transaction ID in the XML
        /// </summary>
        /// <param name="xmlServerResponse"></param>
        /// <returns>Transaction ID if found, otherwise 0</returns>
        [Obsolete("This method is for the legacy XML-based ingest status")]
        public int IngestStepTransactionId(string xmlServerResponse)
        {
            var xmlDoc = new XmlDocument();
            xmlDoc.LoadXml(xmlServerResponse);

            var transactionElement = xmlDoc.SelectSingleNode("//transaction");
            if (transactionElement?.Attributes == null)
            {
                ReportError("IngestStepTransactionId", "transaction element not found in the Status XML");
                return 0;
            }

            var transactionId = int.Parse(transactionElement.Attributes["id"].Value);
            return transactionId;
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

        protected void ReportError(string callingFunction, string message)
        {
            OnErrorMessage(new MessageEventArgs(callingFunction, message));

            ErrorMessage = string.Copy(message);
        }

        #region "Events"

        public event MessageEventHandler DebugEvent;

        protected void OnDebugMessage(MessageEventArgs e)
        {
            DebugEvent?.Invoke(this, e);
        }

        public event MessageEventHandler ErrorEvent;

        protected void OnErrorMessage(MessageEventArgs e)
        {
            ErrorEvent?.Invoke(this, e);
        }

        #endregion

    }
}
