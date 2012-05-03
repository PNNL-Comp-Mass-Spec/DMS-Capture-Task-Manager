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
		#region Private Members

		private BackgroundWorker _topLevelBackgrounder;
		private BackgroundWorker statusBackgrounder;
		private string _bundleIdentifier = string.Empty;
		private const string bundleExtension = ".tar";
		#endregion

		#region Constructor

		public Upload(ref BackgroundWorker backgrounder)
		{
			backgrounder.WorkerReportsProgress = true;
			backgrounder.WorkerSupportsCancellation = true;
			this._topLevelBackgrounder = backgrounder;
			statusBackgrounder = new BackgroundWorker();

			EasyHttp.StatusUpdate += new StatusUpdateEventHandler(EasyHttp_StatusUpdate);
			//TODO - remove
			//EasyHttp.TaskCompleted += new TaskCompletedEventHandler(EasyHttp_TaskCompleted);
		}

		#endregion


		#region Events and Handlers

		public event DebugEventHandler DebugEvent;

		public void RaiseDebugEvent(string callingFunction, string currentTask)
		{
			if (DebugEvent != null)
			{
				DebugEvent(callingFunction, currentTask);
			}
		}

		public event DebugEventHandler ErrorEvent;

		public void RaiseErrorEvent(string callingFunction, string errorMessage)
		{
			if (ErrorEvent != null)
			{
				ErrorEvent(callingFunction, errorMessage);
			}
		}

		private void EasyHttp_StatusUpdate(string bundleIdentifier, int percentCompleted, long totalBytesSent, long totalBytesToSend, string averageUploadSpeed)
		{
			RaiseStatusUpdate(bundleIdentifier, percentCompleted,
					totalBytesSent, totalBytesToSend, averageUploadSpeed);
		}

		public event StatusUpdateEventHandler StatusUpdate;

		public void RaiseStatusUpdate(string bundleIdentifier,
				int percentCompleted, long totalBytesSent,
				long totalBytesToSend, string averageUploadSpeed)
		{
			if (StatusUpdate != null)
			{
				StatusUpdate(bundleIdentifier, percentCompleted, totalBytesSent, totalBytesToSend, averageUploadSpeed);
			}
		}

		public event TaskCompletedEventHandler TaskCompleted;

		private void RaiseTaskCompleted(string bundleIdentifier, string serverResponse)
		{
			if (TaskCompleted != null)
			{
				TaskCompleted(bundleIdentifier, serverResponse);
			}
		}

		public event DataVerifiedHandler DataReceivedAndVerified;

		private void ReportDataReceivedAndVerified(bool success)
		{
			if (DataReceivedAndVerified != null)
			{
				DataReceivedAndVerified(success);
			}
		}

		#endregion

		#region IUpload Members

		public void ProcessMetadata(IDictionary metadataObject)
		{
			NetworkCredential cred = CredentialCache.DefaultNetworkCredentials;
			ProcessMetadata(metadataObject, cred);
		}

		public void ProcessMetadata(IDictionary metadataObject, NetworkCredential loginCredentials)
		{
			string timestamp = DateTime.Now.ToString("yyyyMMddHHmmssffffff");

			string bundleName = timestamp + bundleExtension;
			if (metadataObject.Contains("bundleName") && metadataObject["bundleName"] is string)
			{
				metadataObject["bundleName"] += "_" + bundleName;
			}
			else
			{
				metadataObject.Add("bundleName", bundleName);
			}

			// Grab the list of files from the top-level "file" object
			SortedDictionary<string, FileInfoObject> fileListObject = new SortedDictionary<string, FileInfoObject>();
			IList fileList = (List<Dictionary<string, object>>)metadataObject["file"];
			List<Dictionary<string, object>> newFileObj = new List<Dictionary<string, object>>();

			if (fileList.Count == 0)
			{
				RaiseDebugEvent("ProcessMetadata", "File list is empty; nothing to do");
				RaiseTaskCompleted(bundleName, "");
				return;
			}

			foreach (object file in fileList)
			{
				Dictionary<string, object> fileItem = (Dictionary<string, object>)file;
				string localPath = fileItem["localFilePath"].ToString();
				string relativePath = fileItem["destinationDirectory"].ToString();
				string fileName = fileItem["fileName"].ToString();
				//string fullLocalPath = Path.Combine(localPath, fileName);
				string fullLocalPath = localPath;
				string hash = fileItem["sha1Hash"].ToString();
				FileInfoObject fiObj = new FileInfoObject(fullLocalPath, relativePath, hash);
				fileListObject.Add(fullLocalPath, fiObj);
				newFileObj.Add((Dictionary<string, object>)fiObj.SerializeToDictionaryObject());
			}

			RaiseDebugEvent("ProcessMetadata", "Bundling " + newFileObj.Count + " files");
			metadataObject["file"] = newFileObj;

			string mdJson = Utilities.ObjectToJson(metadataObject);
			DirectoryInfo tmpDir = Utilities.GetTempDirectory();

			string metadataFilename = Path.GetTempFileName();
			FileInfo mdTextFile = new FileInfo(metadataFilename);
			using (StreamWriter sw = mdTextFile.CreateText())
			{
				sw.Write(mdJson);
			}

			FileInfoObject mdFileObject = new FileInfoObject(mdTextFile.FullName, string.Empty);
			mdFileObject.DestinationFileName = "metadata.txt";
			fileListObject.Add(mdFileObject.Sha1HashHex, mdFileObject);

			FileInfo zipFi = BundleFiles(fileListObject, metadataObject["bundleName"].ToString());

			if (Configuration.UploadFiles)
			{
				NetworkCredential newCred = null;
				if (loginCredentials != null)
				{
					newCred = new NetworkCredential(loginCredentials.UserName,
							loginCredentials.Password, loginCredentials.Domain);
				}

				//Get a real server for us to work with
				//string redirectedServer = Utilities.GetRedirect(new Uri(Configuration.TestAuthUri));
				string redirectedServer = Pacifica.Core.Configuration.ServerUri;
				string preallocateUrl = redirectedServer + "/myemsl/cgi-bin/preallocate";

				RaiseDebugEvent("ProcessMetadata", "Preallocating with " + preallocateUrl);
				string preallocateReturn = EasyHttp.Send(preallocateUrl, Auth.GetCookies(), "",
						EasyHttp.HttpMethod.Get, "", false, newCred);

				//TODO - This method really needs to be informed which data upload schemes (e.g. http and https) are allowed
				//The server is the only reliable method to get this information.  'preallocate' needs to return a
				//list of supported upload schemes...
				//Once we know which schemes are supported server side, we can take into consideration user preferences.
				//If the user doesn't mind uploading data unsecured, then that might be preferred as it will speed the upload.
				//For now, we are defaulting to https.
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
				string serverRegex = @"^Server:[\t ]*(?<server>.*)$";
				Match m = Regex.Match(preallocateReturn, serverRegex, RegexOptions.Multiline);
				if (m.Success)
				{
					server = m.Groups["server"].Value.Trim();
				}
				else
				{
					RaiseErrorEvent("ProcessMetadata", "Preallocate did not return a server: " + preallocateReturn);
					throw new ApplicationException(string.Format("Preallocate {0} did not return a server.",
							preallocateUrl));
				}

				string location = null;
				string locRegex = @"^Location:[\t ]*(?<loc>.*)$";
				m = Regex.Match(preallocateReturn, locRegex, RegexOptions.Multiline);
				if (m.Success)
				{
					location = m.Groups["loc"].Value.Trim();
				}
				else
				{
					RaiseErrorEvent("ProcessMetadata", "Preallocate did not return a location: " + preallocateReturn);
					throw new ApplicationException(string.Format("Preallocate {0} did not return a location.",
							preallocateUrl));
				}

				string serverUri = scheme + "://" + server;

				string storageUrl = serverUri + location;

				RaiseDebugEvent("ProcessMetadata", "Sending file to " + storageUrl);
				string resp = EasyHttp.SendFileToDav(location, serverUri, zipFi.FullName, Auth.GetCookies(), newCred);
				string finishUrl = serverUri + "/myemsl/cgi-bin/finish" + location;

				RaiseDebugEvent("ProcessMetadata", "Sending finish via " + finishUrl);
				string finishResult = EasyHttp.Send(finishUrl, Auth.GetCookies(), "",
						EasyHttp.HttpMethod.Get, "", false, newCred);

				//The finish CGI script returns "Location:[URL]\nAccepted\n" on success...
				string finishRegex = @"(^Status:(?<url>.*)\n)?(?<accepted>^Accepted)\n";
				string locationUrl = string.Empty;
				m = Regex.Match(finishResult, finishRegex, RegexOptions.Multiline);
				if (m.Groups["accepted"].Success && !m.Groups["url"].Success)
				{
					RaiseTaskCompleted(bundleName, finishResult);
				}
				else if (m.Groups["accepted"].Success && m.Groups["url"].Success)
				{
					locationUrl = m.Groups["url"].Value.Trim();
					RaiseTaskCompleted(bundleName, locationUrl);
				}
				else
				{
					throw new ApplicationException(finishUrl + " failed with message: " + finishResult);
				}
			}

			mdTextFile.Delete();
		}

		public void BeginUploadMonitoring(string serverStatusURL, string serverSearchURL, IList fileMetadataObject)
		{
			string statusURI = serverStatusURL + "/xml";
			//this.statusBackgrounder.DoWork += new DoWorkEventHandler(UploadMonitorLoop);
			//this.statusBackgrounder.RunWorkerCompleted += new RunWorkerCompletedEventHandler(backgrounder_RunWorkerCompleted);
			Dictionary<string, object> args = new Dictionary<string, object>() { { "statusURI", statusURI }, { "fileMetadataObject", fileMetadataObject }, { "serverSearchURL", serverSearchURL } };
			Boolean success = this.UploadMonitorLoop(args);
			//this.statusBackgrounder.RunWorkerAsync(args);
			DataReceivedAndVerified(success);
		}

		//void  backgrounder_RunWorkerCompleted(object sender, RunWorkerCompletedEventArgs e) {
		//  Boolean success = (bool)e.Result;
		//  DataReceivedAndVerified(success);
		//}

		//void UploadMonitorLoop(object sender, DoWorkEventArgs e) {
		Boolean UploadMonitorLoop(Dictionary<string, object> args)
		{
			//System.ComponentModel.BackgroundWorker bgw = (System.ComponentModel.BackgroundWorker)sender;
			//Dictionary<string, object> args = (Dictionary<string, object>)e.Argument;
			string statusURI = args["statusURI"].ToString();
			string serverSearchURI = args["serverSearchURL"].ToString();
			List<Dictionary<string, object>> fileMetadataObject = (List<Dictionary<string, object>>)args["fileMetadataObject"];

			// Start at a 4 second delay, increase the delay every loop up to 12 times
			// Delay times are:
			//    4 seconds
			//    8 seconds
			//   16 seconds
			//   24 seconds
			//   40 seconds
			//   64 seconds
			//  104 seconds (1.7 minutes)
			//  168 seconds (2.8 minutes)
			//  272 seconds (4.5 minutes)
			//  436 seconds (7.3 minutes)
			//  700 seconds (11.7 minutes)
			// 1124 seconds (18.7 minutes)
			// 1800 seconds (30.0 minutes)
			//
			// Maximum total wait time is 79.3 minutes

			int initialLoopDelaySec = 4;
			int maxLoopCount = 12;
			int loopCount = 0;
			int newDelayTimeSec = 0;
			double delayFactor = 1.6;
			int totalSleepSeconds = 0;

			string xmlServerResponse = string.Empty;

			while (loopCount <= maxLoopCount)
			{
				loopCount++;
				newDelayTimeSec = initialLoopDelaySec * (int)Math.Pow((double)delayFactor, (double)loopCount);

				if (newDelayTimeSec > 10)
				{
					if (newDelayTimeSec < 120)
						RaiseDebugEvent("UploadMonitorLoop", "Sleep for " + newDelayTimeSec + " seconds");
					else
						RaiseDebugEvent("UploadMonitorLoop", "Sleep for " + (newDelayTimeSec / 60.0).ToString("0.0") + " minutes");
				}

				totalSleepSeconds += newDelayTimeSec;

				System.Threading.Thread.Sleep(newDelayTimeSec * 1000);

				try
				{
					xmlServerResponse = EasyHttp.Send(statusURI);
					if (this.WasDataReceived(xmlServerResponse))
					{
						return true;
					}

				}
				catch (Exception ex)
				{
					RaiseErrorEvent("UploadMonitorLoop", ex.Message);
				}
				
				if (loopCount == 1)
					RaiseDebugEvent("UploadMonitorLoop", "Data not yet ready; see " + statusURI);
			}

			RaiseErrorEvent("UploadMonitorLoop", "Data not received after waiting " + (totalSleepSeconds / 60.0).ToString("0.0") + " minutes");

			//e.Result = false;
			return false;
		}

		private Boolean WasDataReceived(string xmlServerResponse)
		{
			Boolean success = false;

			try
			{
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

				// Check the "stored" entry (ID=4) to make sure everything came through ok

				string query = string.Format("//*[@id='{0}']", 4);
				System.Xml.XmlNode statusElement = xmlDoc.SelectSingleNode(query);
				if (statusElement.Attributes["status"].Value.ToLower() == "success" && statusElement.Attributes["message"].Value.ToLower() == "completed")
				{
					success = true;
				}

			}
			catch (Exception ex)
			{
				RaiseErrorEvent("WasDataReceived", ex.Message);
			}

			return success;
		}

		public string GenerateSha1Hash(string fullFilePath)
		{
			return Utilities.GenerateSha1Hash(fullFilePath);
		}

		#endregion

		#region  Member Methods

		private FileInfo BundleFiles(SortedDictionary<string, FileInfoObject> pathList, string bundleId)
		{
			string message = string.Empty;

			ProgressReportingInfo pri = null;

			if (pathList.Count == 0)
			{
				message = "Transport Aborted, no files found";
				return null;
			}
			else
			{
				message = "Preparing Files for Transport";
				pri = new ProgressReportingInfo(bundleId, message, 0, 0, 0, new TimeSpan());
				pri.TotalTaskCount = pathList.Count;
			}

			DirectoryInfo tempDir = Utilities.GetTempDirectory();

			string cacheFileName;
			cacheFileName = bundleId;

			string bundleFilePath = Path.Combine(tempDir.FullName, cacheFileName);

			string statusMessage = "Saving bundle: " + bundleFilePath;
			int percentComplete = 0;
			RaiseStatusUpdate(this._bundleIdentifier, percentComplete, 0, 0, statusMessage);

			EventHandler<SevenZip.ProgressEventArgs> update = (s, e) =>
					{
						statusMessage = "Saving bundle: " + bundleFilePath;
						RaiseStatusUpdate(this._bundleIdentifier, e.PercentDone, 0, 0, statusMessage);
					};

			Utilities.CreateTar(pathList.Values, bundleFilePath, update);

			statusMessage = "Save bundle complete:";
			percentComplete = 100;
			RaiseStatusUpdate(this._bundleIdentifier, percentComplete, 0, 0, statusMessage);

			FileInfo retFi = new FileInfo(bundleFilePath);
			string newPath = Path.Combine(tempDir.FullName, cacheFileName);
			retFi.MoveTo(newPath);

			retFi = new FileInfo(newPath);
			Trace.WriteLine(retFi.FullName);
			return retFi;
		}

		#endregion
	}
}