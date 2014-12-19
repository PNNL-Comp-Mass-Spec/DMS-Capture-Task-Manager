using System;
using System.Collections.Generic;
using System.Net;
using System.Text.RegularExpressions;
using System.Xml;

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

		public enum StatusStep
		{
			Submitted = 0,		// .tar file submitted
			Received = 1,		// .tar file received
			Processing = 2,		// .tar file being processed
			Verified = 3,		// .tar file contents validated
			Stored = 4,			// .tar file contents copied to Aurora
			Available = 5,		// Available in Elastic Search
			Archived = 6		// Sha-1 hash values of files in Aurora validated against expected hash values
		}

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
		}

        /// <summary>
        /// Obtain the XML returned by the given MyEMSL status page
        /// </summary>
        /// <param name="statusURI">URI to examine</param>
        /// <param name="lookupError">Output: true if an error occurs</param>
        /// <param name="errorMessage">Output: error message if lookupError is true</param>
        /// <returns>Status message, in XML format; empty string if an error</returns>
        public string GetIngestStatus(string statusURI, out bool lookupError, out string errorMessage)
        {

            // Call the testauth service to obtain a cookie for this session
            string authURL = Configuration.TestAuthUri;
            var auth = new Auth(new Uri(authURL));

            CookieContainer cookieJar;
            if (!auth.GetAuthCookies(out cookieJar))
            {
                lookupError = true;
                errorMessage = "Auto-login to " + Configuration.TestAuthUri + " failed authentication";
                ReportError("GetIngestStatus", errorMessage);
                return string.Empty;
            }

            var xmlServerReponse = GetIngestStatus(statusURI, cookieJar, out lookupError, out errorMessage);

            Utilities.Logout(cookieJar);

            return xmlServerReponse;
        }

        /// <summary>
        /// Obtain the XML returned by the given MyEMSL status page
        /// </summary>
        /// <param name="statusURI">URI to examine</param>
        /// <param name="cookieJar">Cookies</param>
        /// <param name="lookupError">Output: true if an error occurs</param>
        /// <param name="errorMessage">Output: error message if lookupError is true</param>
        /// <returns>Status message, in XML format; empty string if an error</returns>
        public string GetIngestStatus(string statusURI,
            CookieContainer cookieJar,
            out bool lookupError,
            out string errorMessage)
        {
            const string EXCEPTION_TEXT = "message=\'exceptions.";

            lookupError = false;
            errorMessage = string.Empty;

            // The following Callback allows us to access the MyEMSL server even if the certificate is expired or untrusted
            // For more info, see comments in Upload.StartUpload()
            if (ServicePointManager.ServerCertificateValidationCallback == null)
                ServicePointManager.ServerCertificateValidationCallback += Utilities.ValidateRemoteCertificate;

            const int timeoutSeconds = 30;
            HttpStatusCode responseStatusCode;

            string xmlServerResponse = EasyHttp.Send(statusURI, out responseStatusCode, timeoutSeconds);

            int exceptionIndex = xmlServerResponse.IndexOf(EXCEPTION_TEXT);
            if (exceptionIndex <= 0)
            {
                return xmlServerResponse;
            }

            string message = xmlServerResponse.Substring(exceptionIndex + EXCEPTION_TEXT.Length);
            int charIndex = message.IndexOf("traceback");
            if (charIndex > 0)
                message = message.Substring(0, charIndex - 1).Replace("\n", "; ").Replace("&lt", "");
            else
            {
                charIndex = message.IndexOf('\'', 5);
                if (charIndex > 0)
                    message = message.Substring(0, charIndex - 1).Replace("\n", "; ");
            }

            errorMessage = "Exception: " + message;
            lookupError = true;

            return string.Empty;
        }

        protected bool HasExceptions(string xmlServerResponse, bool reportError, out string errorMessage)
        {
            const string EXCEPTION_TEXT = "message=\'exceptions.";
            errorMessage = string.Empty;

            int exceptionIndex = xmlServerResponse.IndexOf(EXCEPTION_TEXT);
            if (exceptionIndex <= 0)
            {
                return false;
            }

            string exceptionMessage = xmlServerResponse.Substring(exceptionIndex + EXCEPTION_TEXT.Length);
            int charIndex = exceptionMessage.IndexOf("traceback");
            if (charIndex > 0)
                exceptionMessage = exceptionMessage.Substring(0, charIndex - 1).Replace("\n", "; ").Replace("&lt", "");
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
            var reGetStatusNum = new Regex(@"(\d+)/xml", RegexOptions.IgnoreCase);
            var reMatch = reGetStatusNum.Match(statusURI);
            if (!reMatch.Success)
                throw new Exception("Could not find Status ID in StatusURI: " + statusURI);

            int statusNum;
            if (!int.TryParse(reMatch.Groups[1].Value, out statusNum))
                throw new Exception("Status ID is not numeric in StatusURI: " + statusURI);

            if (statusNum <= 0)
                throw new Exception("Status ID is 0 in StatusURI: " + statusURI);

            return statusNum;
        }

        /// <summary>
        /// Examines the status of each step in xmlServerResponse to see if any of them contain status Error
        /// </summary>
        /// <param name="xmlServerResponse"></param>
        /// <param name="errorMessage">Output: error messge</param>
        /// <returns>True if an error, false if no errors</returns>
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
                    int stepNumber = -1;
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
		/// This function examines the xml returned by a MyEMSL status page to determine whether or not the step succeeded
		/// </summary>
        /// <param name="xmlServerResponse"></param>
		/// <param name="stepNum">Step number whose status should be examined</param>
        /// <param name="statusMessage">Output parameter: status message for step stepNum</param>
        /// <param name="errorMessage">Output parameter: status message for step stepNum</param>
		/// <returns>True if step stepNum has successfully completed</returns>
		public bool IngestStepCompleted(
            string xmlServerResponse,
			StatusStep stepNum,
            out string statusMessage,
			out string errorMessage)
		{
            const string UPLOAD_PERMISSION_ERROR = "do not have upload permissions";

            statusMessage = string.Empty;
            errorMessage = string.Empty;

	        // First look for exceptions
		    if (HasExceptions(xmlServerResponse, true, out errorMessage))
		    {
                // Exceptions are present; step is not complete
                return false;
		    }

		    var xmlDoc = new XmlDocument();
			xmlDoc.LoadXml(xmlServerResponse);

			// Example XML:
			//
			// <?xml version="1.0"?>
			// <myemsl>
			// 	<status username='70000'>
			// 		<transaction id='111177' />
			// 		<step id='0' message='completed' status='SUCCESS' />
			// 		<step id='1' message='completed' status='SUCCESS' />
			// 		<step id='2' message='completed' status='SUCCESS' />
			// 		<step id='3' message='completed' status='SUCCESS' />
			// 		<step id='4' message='completed' status='SUCCESS' />
			// 		<step id='5' message='completed' status='SUCCESS' />
			// 		<step id='6' message='verified' status='SUCCESS' />
			// 	</status>
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
			// 6: Archived    (status will be "UNKNOWN" if not yet verified)

			string query = string.Format("//step[@id='{0}']", (int)stepNum);
			XmlNode statusElement = xmlDoc.SelectSingleNode(query);

		    if (statusElement == null || statusElement.Attributes == null)
		    {
		        errorMessage = "Match not found for step " + stepNum + " in the Status XML";
		        ReportError("IngestStepCompleted", errorMessage);
		        return false;
		    }

		    string message = statusElement.Attributes["message"].Value;
		    string status = statusElement.Attributes["status"].Value;

		    if (string.IsNullOrEmpty(message))
		    {
                errorMessage = "message attribute in the Status XML is empty for step " + stepNum;
                return false;
		    }

            if (string.IsNullOrEmpty(status))
            {
                errorMessage = "status attribute in the Status XML is empty for step " + stepNum;
                return false;
            }

		    if (status.ToLower() == "error")
		    {

                if (message.Contains(UPLOAD_PERMISSION_ERROR))
                    errorMessage = PERMISSIONS_ERROR + " " + message;
                else
		            errorMessage = message;

                return false;
		    }

		    if (status.ToLower() == "success")
		    {
		        if (message.ToLower() == "completed")
		        {
		            statusMessage = "Step is complete";
                    return true;
		        }

		        if (message.ToLower() == "verified")
		        {
		            statusMessage = "Data is verified";
                    return true;
		        }

                return false;
		    }

		    if (status.ToLower() == "unknown")
		    {
                // Step is not yet complete
		        statusMessage = "Waiting";
                return false;
		    }

		    // Status is not empty, error, success, or unknown
            // Unrecognized state

            errorMessage = "Unrecognized status state: " + status;
            return false;
		    
		}

        public byte IngestStepCompletionCount(string xmlServerResponse)
        {
            string errorMessage;

	        // First look for exceptions
		    if (HasExceptions(xmlServerResponse, false, out errorMessage))
		    {
                // Exceptions are present; report 0 steps complete
                return 0;
		    }

		    var xmlDoc = new XmlDocument();
			xmlDoc.LoadXml(xmlServerResponse);

            // Find all step elements that contain an id attribute
            // See function IngestStepCompleted for Example XML
            XmlNodeList stepNodes = xmlDoc.SelectNodes("//step[@id]");

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

                string message = stepNode.Attributes["message"].Value;
                string status = stepNode.Attributes["status"].Value;

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

		public event MessageEventHandler ErrorEvent;

		public void OnErrorMessage(MessageEventArgs e)
		{
			if (ErrorEvent != null)
				ErrorEvent(this, e);
		}

		#endregion
       
    }
}
