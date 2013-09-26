using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;

namespace Pacifica.Core
{
	public class MyEMSLStatusCheck
	{
		public enum StatusStep
		{
			Submitted=0,		// .tar file submitted
			Received=1,			// .tar file received
			Processing=2,		// .tar file being processed
			Verified=3,			// .tar file contents validated
			Stored=4,			// .tar file contents copied to Aurora
			Available=5,		// Available in Elastic Search
			Archived=6			// Sha-1 hash values of files in Aurora validated against expected hash values
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
			this.ErrorMessage = string.Empty;
		}

		/// <summary>
		/// This function examines the xml returned by a MyEMSL status page to determine whether or not the step succeeded
		/// </summary>
		/// <param name="statusURI"></param>
		/// <param name="stepNum"></param>
		/// <param name="abortNow"></param>
		/// <param name="statusMessage"></param>
		/// <returns>True if step stepNum has completed</returns>
		public bool IngestStepCompleted(string statusURI, StatusStep stepNum, out bool accessDenied, out string statusMessage)
		{
			bool success = false;
			accessDenied = false;
			statusMessage = string.Empty;

			// Call the testauth service to obtain a cookie for this session
			string authURL = Configuration.TestAuthUri;
			Auth auth = new Auth(new Uri(authURL));

			CookieContainer cookieJar = null;
			if (!auth.GetAuthCookies(out cookieJar))
			{
				string msg = "Auto-login to " + Configuration.TestAuthUri + " failed authentication";
				ReportError("CheckMyEMSLIngestStatus", msg);
				return false;
			}

			success = IngestStepCompleted(statusURI, stepNum, cookieJar, out accessDenied, out statusMessage);

			Utilities.Logout(cookieJar);

			return success;
		}

		/// <summary>
		/// This function examines the xml returned by a MyEMSL status page to determine whether or not the step succeeded
		/// </summary>
		/// <param name="statusURI"></param>
		/// /// <param name="stepNum"></param>
		/// <param name="accessDenied"></param>
		/// <param name="statusMessage"></param>
		/// <param name="cookieJar"></param>
		/// <returns>True if step stepNum has completed</returns>
		public bool IngestStepCompleted(string statusURI, StatusStep stepNum, CookieContainer cookieJar, out bool accessDenied, out string statusMessage)
		{
			bool success = false;
			accessDenied = false;
			statusMessage = string.Empty;

			int timeoutSeconds = 5;
			HttpStatusCode responseStatusCode;

			string xmlServerResponse = EasyHttp.Send(statusURI, out responseStatusCode, timeoutSeconds);

			System.Xml.XmlDocument xmlDoc = new System.Xml.XmlDocument();
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

			string query = string.Format("//*[@id='{0}']", (int)stepNum);
			System.Xml.XmlNode statusElement = xmlDoc.SelectSingleNode(query);

			string message = statusElement.Attributes["message"].Value;
			string status = statusElement.Attributes["status"].Value;

			if (status.ToLower() == "success")
			{
				if (message.ToLower() == "completed")
				{
					statusMessage = "Step is complete";
					success = true;
				}

				if (message.ToLower() == "verified")
				{
					statusMessage = "Data is verified";
					success = true;
				}
			}

			if (!success && message.Contains("do not have upload permissions"))
			{
				statusMessage = "Permissions error: " + message;
				accessDenied = true;
			}

			return success;
		}

		protected void ReportError(string callingFunction, string message)
		{
			OnErrorMessage(new MessageEventArgs(callingFunction, message));

			this.ErrorMessage = string.Copy(message);
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
