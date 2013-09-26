using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Text.RegularExpressions;

namespace Pacifica.Core
{
	public class Upload : IUpload
	{
		public struct udtUploadMetadata
		{
			public int DatasetID;
			public int DataPackageID;
			public string SubFolder;
			public string DatasetName;			// Only used for datasets; not Data Packages
			public string DateCodeString;		// Only used for datasets; not Data Packages
			public string DMSInstrumentName;	// Only used for datasets; not Data Packages
			public string EUSInstrumentID;		// Only used for datasets; not Data Packages
			public string EUSProposalID;		// Only used for datasets; not Data Packages

			public void Clear()
			{
				DatasetID = 0;
				DataPackageID = 0;
				SubFolder = string.Empty;
				DatasetName = string.Empty;
				DateCodeString = string.Empty;
				DMSInstrumentName = string.Empty;
				EUSInstrumentID = string.Empty;
				EUSProposalID = string.Empty;
			}
		}

		#region Private Members

		private string _bundleIdentifier = string.Empty;
		private const string bundleExtension = ".tar";

		private CookieContainer mCookieJar;

		private string mTransferFolderPath;
		private string mJobNumber;

		#endregion

		#region Constructor

		public static Dictionary<string, object> CreateMetadataObject(Upload.udtUploadMetadata uploadMetadata, List<FileInfoObject> lstUnmatchedFiles)
		{
			// Keys in this object are key names; values are either strings or dictionary objects or even a list of dictionary objects
			Dictionary<string, object> metadataObject = new Dictionary<string, object>();
			var groupObject = new List<Dictionary<string, string>>();

			// Set up the MyEMSL tagging information

			if (uploadMetadata.DatasetID > 0)
			{
				groupObject.Add(new Dictionary<string, string>() {
					{ "name", uploadMetadata.DMSInstrumentName }, { "type", "omics.dms.instrument" } });
				groupObject.Add(new Dictionary<string, string>() {
					{ "name", uploadMetadata.DateCodeString }, { "type", "omics.dms.date_code" } });
				groupObject.Add(new Dictionary<string, string>() {
					{ "name", uploadMetadata.DatasetName }, { "type", "omics.dms.dataset" } });
				groupObject.Add(new Dictionary<string, string>() {
					{ "name", uploadMetadata.DatasetID.ToString() }, { "type", "omics.dms.dataset_id" } });
			}
			else if (uploadMetadata.DataPackageID > 0)
			{
				groupObject.Add(new Dictionary<string, string>() {
					{ "name", uploadMetadata.DataPackageID.ToString() }, { "type", "omics.dms.datapackage_id" } });
			}
			else
			{
				throw new ArgumentOutOfRangeException("Must define a DatasetID or a DataPackageID; cannot create the metadata object");
			}

			var eusInfo = new Dictionary<string, object>();

			eusInfo.Add("groups", groupObject);

			if (uploadMetadata.DatasetID > 0)
			{
				if (string.IsNullOrWhiteSpace(uploadMetadata.EUSInstrumentID))
				{
					// This instrument does not have an EUS_Instrument_ID
					// Use 34127, which is VOrbiETD04
					eusInfo.Add("instrumentId", "34127");
				}
				else
				{
					eusInfo.Add("instrumentId", uploadMetadata.EUSInstrumentID);
				}

				eusInfo.Add("instrumentName", uploadMetadata.DMSInstrumentName);

				if (string.IsNullOrWhiteSpace(uploadMetadata.EUSProposalID))
				{
					// This dataset does not have an EUS_Proposal_ID
					// Use 17797, which is "Development of High Throughput Proteomic Production Operations"
					eusInfo.Add("proposalID", "17797");
				}
				else
				{
					eusInfo.Add("proposalID", uploadMetadata.EUSProposalID);
				}
			}

			metadataObject.Add("bundleName", "omics_dms");
			metadataObject.Add("creationDate", Pacifica.Core.ExtensionMethods.ToUnixTime(DateTime.UtcNow).ToString());
			metadataObject.Add("eusInfo", eusInfo);

			metadataObject.Add("file", lstUnmatchedFiles);

			metadataObject.Add("version", "1.2.0");

			return metadataObject;
		}

		/// <summary>
		/// Constructor
		/// </summary>
		/// <param name="transferFolderPath">Transfer foler path for this dataset, for example \\proto-4\DMS3_Xfer\SysVirol_IFT001_Pool_17_B_10x_27Aug13_Tiger_13-07-36</param>
		/// <param name="mJob">DMS Data Capture job number</param>
		/// <remarks>The metadata.txt file will be copied to the transfer folder</remarks>
		public Upload(string transferFolderPath, string jobNumber)
		{

			// Note that EasyHttp is a static class with a static event
			// Be careful about instantiating this class (Upload) multiple times
			EasyHttp.StatusUpdate += new StatusUpdateEventHandler(EasyHttp_StatusUpdate);

			mTransferFolderPath = transferFolderPath;
			mJobNumber = jobNumber;
		}

		#endregion


		#region Events and Handlers

		public event MessageEventHandler DebugEvent;
		public event MessageEventHandler ErrorEvent;
		public event UploadCompletedEventHandler UploadCompleted;
		public event StatusUpdateEventHandler StatusUpdate;


		public void RaiseDebugEvent(string callingFunction, string currentTask)
		{
			if (DebugEvent != null)
			{
				DebugEvent(this, new MessageEventArgs(callingFunction, currentTask));
			}
		}

		public void RaiseErrorEvent(string callingFunction, string errorMessage)
		{
			if (ErrorEvent != null)
			{
				ErrorEvent(this, new MessageEventArgs(callingFunction, errorMessage));
			}
		}


		void EasyHttp_StatusUpdate(object sender, StatusEventArgs e)
		{
			if (StatusUpdate != null)
			{
				StatusUpdate(this, e);
			}
		}

		private void RaiseUploadCompleted(string serverResponse)
		{
			if (UploadCompleted != null)
			{
				UploadCompleted(this, new UploadCompletedEventArgs(serverResponse));
			}
		}

		#endregion

		#region IUpload Members

		public bool StartUpload(Dictionary<string, object> metadataObject, out string statusURL)
		{
			NetworkCredential cred = null;
			return StartUpload(metadataObject, cred, out statusURL);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="metadataObject"></param>
		/// <param name="loginCredentials"></param>
		/// <param name="?"></param>
		/// <returns>True if successfully uploaded, false if an error</returns>
		public bool StartUpload(Dictionary<string, object> metadataObject, NetworkCredential loginCredentials, out string statusURL)
		{
			string timestamp = DateTime.Now.ToString("yyyyMMddHHmmssffffff");
			statusURL = string.Empty;

			var fileList = (List<FileInfoObject>)metadataObject["file"];

			// Grab the list of files from the top-level "file" object
			// Keys in this dictionary are the source file path; values are metadata about the file
			SortedDictionary<string, FileInfoObject> fileListObject = new SortedDictionary<string, FileInfoObject>();

			// This is a list of dictionary objects
			// Dictionary keys will be sha1Hash, destinationDirectory, and fileName
			var newFileObj = new List<Dictionary<string, string>>();

			foreach (var file in fileList)
			{

				var fiObj = new FileInfoObject(file.AbsoluteLocalPath, file.RelativeDestinationDirectory, file.Sha1HashHex);

				fileListObject.Add(file.AbsoluteLocalPath, fiObj);
				newFileObj.Add(fiObj.SerializeToDictionaryObject());

			}

			metadataObject["file"] = newFileObj;

			string mdJson = Utilities.ObjectToJson(metadataObject);

			// Create the metadata.txt file
			string metadataFilename = Path.GetTempFileName();
			FileInfo mdTextFile = new FileInfo(metadataFilename);
			using (StreamWriter sw = mdTextFile.CreateText())
			{
				sw.Write(mdJson);
			}

			try
			{
				// Copy the Metadata.txt file to the transfer folder, then delete the local copy
				if (!string.IsNullOrEmpty(mTransferFolderPath))
				{
					var fiTargetFile = new FileInfo(Path.Combine(mTransferFolderPath, Utilities.GetMetadataFilenameForJob(mJobNumber)));
					if (!fiTargetFile.Directory.Exists)
						fiTargetFile.Directory.Create();

					mdTextFile.CopyTo(fiTargetFile.FullName, true);
				}

			}
			catch
			{
				// Ignore errors here
			}


			if (fileList.Count == 0)
			{
				RaiseDebugEvent("ProcessMetadata", "File list is empty; nothing to do");
				RaiseUploadCompleted("");
				return true;
			}

			NetworkCredential newCred = null;
			if (loginCredentials != null)
			{
				newCred = new NetworkCredential(loginCredentials.UserName,
						loginCredentials.Password, loginCredentials.Domain);
			}

			// Call the testauth service to obtain a cookie for this session
			string authURL = Configuration.TestAuthUri;
			Auth auth = new Auth(new Uri(authURL));

			mCookieJar = null;
			if (!auth.GetAuthCookies(out mCookieJar))
			{
				string msg = "Auto-login to " + Configuration.TestAuthUri + " failed authentication";
				RaiseErrorEvent("ProcessMetadata", msg);
				throw new ApplicationException(msg);
			}

			string redirectedServer = Configuration.IngestServerUri;
			string preallocateUrl = redirectedServer + "/myemsl/cgi-bin/preallocate";
			int timeoutSeconds = 10;


			RaiseDebugEvent("ProcessMetadata", "Preallocating with " + preallocateUrl);
			string postData = "";
			HttpStatusCode responseStatusCode;

			string preallocateReturn = EasyHttp.Send(preallocateUrl, mCookieJar,
				out responseStatusCode, postData,
				EasyHttp.HttpMethod.Get, timeoutSeconds, newCred);

			string scheme = "http";

			//This is just a local configuration that states which is preferred.
			//It doesn't inform what is supported on the server.
			if (Configuration.UseSecureDataTransfer)
			{
				scheme = Configuration.SecuredScheme;
			}
			else
			{
				scheme = Configuration.UnsecuredScheme;
			}

			string server = null;
			var reServerName = new Regex(@"^Server:[\t ]*(?<server>.*)$", RegexOptions.Multiline);
			Match m = reServerName.Match(preallocateReturn);

			if (m.Success)
			{
				server = m.Groups["server"].Value.Trim();
			}
			else
			{
				Utilities.Logout(mCookieJar);
				RaiseErrorEvent("ProcessMetadata", "Preallocate did not return a server: " + preallocateReturn);
				throw new ApplicationException(string.Format("Preallocate {0} did not return a server.",
						preallocateUrl));
			}

			string location = null;
			var reLocation = new Regex(@"^Location:[\t ]*(?<loc>.*)$", RegexOptions.Multiline);

			m = reLocation.Match(preallocateReturn);
			if (m.Success)
			{
				location = m.Groups["loc"].Value.Trim();
			}
			else
			{
				Utilities.Logout(mCookieJar);
				RaiseErrorEvent("ProcessMetadata", "Preallocate did not return a location: " + preallocateReturn);
				throw new ApplicationException(string.Format("Preallocate {0} did not return a location.",
						preallocateUrl));
			}

			string serverUri = scheme + "://" + server;

			string storageUrl = serverUri + location;

			RaiseDebugEvent("ProcessMetadata", "Sending file to " + storageUrl);

			// The response data will likely be empty
			//string resp = EasyHttp.SendFileToDav(location, serverUri, fiTarFile.FullName, mCookieJar, newCred);
			string resp = EasyHttp.SendFileListToDavAsTar(location, serverUri, fileListObject, mdTextFile.FullName, mCookieJar, newCred);

			string finishUrl = serverUri + "/myemsl/cgi-bin/finish" + location;
			RaiseDebugEvent("ProcessMetadata", "Sending finish via " + finishUrl);
			timeoutSeconds = 10;
			postData = "";

			string finishResult = EasyHttp.Send(finishUrl, mCookieJar,
				out responseStatusCode, postData,
				EasyHttp.HttpMethod.Get, timeoutSeconds, newCred);

			// The finish CGI script returns "Status:[URL]\nAccepted\n" on success...
			// This RegEx looks for Accepted in the text, optionally preceded by a Status: line
			var reStatusURL = new Regex(@"(^Status:(?<url>.*)\n)?(?<accepted>^Accepted)\n", RegexOptions.Multiline);
			bool success = false;

			m = reStatusURL.Match(finishResult);
			if (m.Groups["accepted"].Success && !m.Groups["url"].Success)
			{
				// File was accepted, but the Status URL is empty
				// This likely indicates a problem
				RaiseUploadCompleted(finishResult);
				success = false;
			}
			else if (m.Groups["accepted"].Success && m.Groups["url"].Success)
			{
				statusURL = m.Groups["url"].Value.Trim();
				RaiseUploadCompleted(statusURL);
				success = true;
			}
			else
			{
				Utilities.Logout(mCookieJar);
				throw new ApplicationException(finishUrl + " failed with message: " + finishResult);
			}

			try
			{
				// Delete the local temporary file
				mdTextFile.Delete();
			}
			catch
			{
				// Ignore errors here
			}

			Utilities.Logout(mCookieJar);
			return success;
		}

		public string GenerateSha1Hash(string fullFilePath)
		{
			return Utilities.GenerateSha1Hash(fullFilePath);
		}

		#endregion

		#region  Member Methods

		#endregion

	}

}