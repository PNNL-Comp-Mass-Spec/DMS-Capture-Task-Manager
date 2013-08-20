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

		/* August 2013: To be deleted
		 *
		 * private BackgroundWorker _topLevelBackgrounder;
		 * private BackgroundWorker statusBackgrounder;
		 */

		private string _bundleIdentifier = string.Empty;
		private const string bundleExtension = ".tar";

		#endregion

		#region Constructor

		public Upload(ref BackgroundWorker backgrounder)
		{
			backgrounder.WorkerReportsProgress = true;
			backgrounder.WorkerSupportsCancellation = true;

			/* August 2013: To be deleted
			 *
			 * this._topLevelBackgrounder = backgrounder;
			 * statusBackgrounder = new BackgroundWorker();
			 */

			// Note that EasyHttp is a static class with a static event
			// Be careful about instantiating this class (Upload) multiple times
			EasyHttp.StatusUpdate += new StatusUpdateEventHandler(EasyHttp_StatusUpdate);
			
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
			RaiseStatusUpdate(bundleIdentifier, percentCompleted, totalBytesSent, totalBytesToSend, averageUploadSpeed);
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

		private void ReportDataReceivedAndVerified(bool success, string errorMessage)
		{
			if (DataReceivedAndVerified != null)
			{
				DataReceivedAndVerified(success, errorMessage);
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

			// ToDo: Add "subdir" key and value

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

				if (fiObj.RelativeDestinationFullPath.ToLower() == "metadata.txt")
				{
					// We must skip this file since MyEMSL stores a special metadata.txt file at the root
					RaiseErrorEvent("ProcessMetadata", "Skipping metadata.txt file at '" + fiObj.AbsoluteLocalPath + "' due to name conflict with the MyEmsl metadata.txt file");
				}
				else
				{
					fileListObject.Add(fullLocalPath, fiObj);
					newFileObj.Add(fiObj.SerializeToDictionaryObject());
				}
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

			// Prior to June 2012, we would add the metadata.txt file to fileListObject
			// We now send that information into BundleFiles using the metadataFilePath parameter
			// Deprecated code:
			//  FileInfoObject mdFileObject = new FileInfoObject(mdTextFile.FullName, string.Empty);
			//  mdFileObject.DestinationFileName = "metadata.txt";
			//  fileListObject.Add(mdFileObject.Sha1HashHex, mdFileObject);

			FileInfo zipFi = BundleFiles(fileListObject, mdTextFile.FullName, metadataObject["bundleName"].ToString());

			if (Configuration.UploadFiles)
			{
				NetworkCredential newCred = null;
				if (loginCredentials != null)
				{
					newCred = new NetworkCredential(loginCredentials.UserName,
							loginCredentials.Password, loginCredentials.Domain);
				}

				/* August 2013: To be deleted
				 *
				 * Get a real server for us to work with
				 * string redirectedServer = Utilities.GetRedirect(new Uri(Configuration.TestAuthUri));
				 */

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

			/* August 2013: To be deleted
			 *
			 * this.statusBackgrounder.DoWork += new DoWorkEventHandler(UploadMonitorLoop);
			 * this.statusBackgrounder.RunWorkerCompleted += new RunWorkerCompletedEventHandler(backgrounder_RunWorkerCompleted);
			 */

			Dictionary<string, object> args = new Dictionary<string, object>() { { "statusURI", statusURI }, { "fileMetadataObject", fileMetadataObject }, { "serverSearchURL", serverSearchURL } };
			string errorMessage;
			Boolean success = this.UploadMonitorLoop(args, out errorMessage);

			/* August 2013: To be deleted
			 *
			 * //this.statusBackgrounder.RunWorkerAsync(args);
			 */

			DataReceivedAndVerified(success, errorMessage);
		}

		/* August 2013: To be deleted
		 *
		 * void  backgrounder_RunWorkerCompleted(object sender, RunWorkerCompletedEventArgs e) {
		 *   Boolean success = (bool)e.Result;
		 *   DataReceivedAndVerified(success);
		 * }
		
		 * void UploadMonitorLoop(object sender, DoWorkEventArgs e) {
		*/

		Boolean UploadMonitorLoop(Dictionary<string, object> args, out string errorMessage)
		{
			//System.ComponentModel.BackgroundWorker bgw = (System.ComponentModel.BackgroundWorker)sender;
			//Dictionary<string, object> args = (Dictionary<string, object>)e.Argument;
			string statusURI = args["statusURI"].ToString();
			string serverSearchURI = args["serverSearchURL"].ToString();
			List<Dictionary<string, object>> fileMetadataObject = (List<Dictionary<string, object>>)args["fileMetadataObject"];
			errorMessage = string.Empty;

			// Start at a 4 second delay, increase the delay every loop until the delay is 120 seconds
			// Maximum wait time is 90 minutes

			int currentLoopDelaySec = 4;
			int maxLoopDelaySec = 300;		// 5 minutes
			int iterations = 1;
			int maxWaitTimeMinutes = 90;

			string xmlServerResponse = string.Empty;
			bool abortNow;
			string dataReceivedMessage;
			System.DateTime dtStartTime = System.DateTime.UtcNow;

			while (System.DateTime.UtcNow.Subtract(dtStartTime).TotalMinutes < maxWaitTimeMinutes)
			{
				if (currentLoopDelaySec > 10)
				{
					RaiseDebugEvent("UploadMonitorLoop", "Waiting " + currentLoopDelaySec + " seconds");
				}

				System.Threading.Thread.Sleep(currentLoopDelaySec * 1000);

				try
				{
					xmlServerResponse = EasyHttp.Send(statusURI);
					if (this.WasDataReceived(xmlServerResponse, out abortNow, out dataReceivedMessage))
					{
						return true;
					}

					if (abortNow)
					{
						errorMessage = string.Copy(dataReceivedMessage);
						return false;
					}

				}
				catch (Exception ex)
				{
					RaiseErrorEvent("UploadMonitorLoop", ex.Message);
				}
				
				if (iterations == 1)
					RaiseDebugEvent("UploadMonitorLoop", "Data not yet ready; see " + statusURI);

				iterations++;
				if (currentLoopDelaySec < maxLoopDelaySec)
				{
					currentLoopDelaySec *= 2;
					if (currentLoopDelaySec > maxLoopDelaySec)
						currentLoopDelaySec = maxLoopDelaySec;
				}

			}

			RaiseErrorEvent("UploadMonitorLoop", "Data not received after waiting " + System.DateTime.UtcNow.Subtract(dtStartTime).TotalMinutes.ToString("0.0") + " minutes");

			//e.Result = false;
			return false;
		}

		private Boolean WasDataReceived(string xmlServerResponse, out bool abortNow, out string dataReceivedMessage)
		{
			Boolean success = false;
			abortNow = false;
			dataReceivedMessage = string.Empty;

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

				// Check the "available" entry (ID=5) to make sure everything came through ok

				string query = string.Format("//*[@id='{0}']", 5);
				System.Xml.XmlNode statusElement = xmlDoc.SelectSingleNode(query);

				string message = statusElement.Attributes["message"].Value;
				string status = statusElement.Attributes["status"].Value;

				if (status.ToLower() == "success" && message.ToLower() == "completed")
				{
					dataReceivedMessage = "Data is available";
					success = true;
				}

				if (message.Contains("do not have upload permissions"))
				{
					dataReceivedMessage = "Aborting upload due to permissions error: " + message;
					abortNow = true;
				}

			}
			catch (Exception ex)
			{
				RaiseErrorEvent("WasDataReceived", ex.Message);
			}

			if (success)
			{
				string logoutURL = Configuration.ServerUri + "/myemsl/logout";
				string response = EasyHttp.Send(logoutURL, Auth.GetCookies(), "", EasyHttp.HttpMethod.Get, "", false, null);
			}

			return success;
		}

		public string GenerateSha1Hash(string fullFilePath)
		{
			return Utilities.GenerateSha1Hash(fullFilePath);
		}

		#endregion

		#region  Member Methods

		private FileInfo BundleFiles(SortedDictionary<string, FileInfoObject> pathList, string metadataFilePath, string bundleId)
		{
			string message = string.Empty;

			if (pathList.Count == 0)
			{
				message = "Transport Aborted, no files found";
				return null;
			}
			else
			{
				message = "Preparing Files for Transport";
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

			CreateTar(pathList.Values, metadataFilePath, bundleFilePath, update);		

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

		/// <summary>
		/// Create a Tar file using SevenZipSharp
		/// Compression rate is ~2 GB/minute
		/// </summary>
		/// <param name="files"></param>
		/// <param name="metadataFilePath"></param>
		/// <param name="outputFilePath"></param>
		/// <param name="warningMessages"></param>
		private void CreateTar(IEnumerable<FileInfoObject> sourceFilePaths, string metadataFilePath, string outputFilePath,
			EventHandler<SevenZip.ProgressEventArgs> progressUpdate)
		{

			string path = System.Reflection.Assembly.GetExecutingAssembly().Location;
			System.IO.FileInfo fi = new System.IO.FileInfo(path);
			path = System.IO.Path.Combine(fi.DirectoryName, "7z.dll");
			SevenZip.SevenZipCompressor.SetLibraryPath(path);
			
			// This list tracks the full paths of the files to be added to the .tar file
			// We pass this list to the compressor when using function CompressFiles()
			System.Collections.Generic.List<string> sourceFilePathsFull = new System.Collections.Generic.List<string>();
			
			// This dictionary keeps track of the target file paths within the .tar file
			// Key is the target file path, while value is the source file path
			// We pass this dictionary to the compressor when using function CompressFileDictionary()
			Dictionary<string, string> fileDict = new Dictionary<string, string>();

			SevenZip.SevenZipCompressor compressor = new SevenZip.SevenZipCompressor();
			compressor.Compressing += progressUpdate;
			compressor.ArchiveFormat = SevenZip.OutArchiveFormat.Tar;

			// First create the tar file using the files in sourceFilePaths
			// Populate a generic list with the full paths to the source files, then call compressor.CompressFiles() 
			// This function will examine the source files to determine the common path that they all share
			// It will remove that common path when storing the files in the .tar file
			using (System.IO.FileStream tarFileStream = System.IO.File.Create(outputFilePath))
			{

				string dictionaryValue = string.Empty;
				fileDict.Clear();

				foreach (FileInfoObject file in sourceFilePaths)
				{
					if (fileDict.TryGetValue(file.RelativeDestinationFullPath, out dictionaryValue))
					{
						RaiseErrorEvent("CreateTar", "Skipped file '" + file.RelativeDestinationFullPath + "' since already present in dictionary fileDict.  Existing entry has value '" + dictionaryValue + "' while new item has value '" + file.AbsoluteLocalPath + "'");
					}
					else
					{
						if (string.Compare(file.RelativeDestinationFullPath.ToLower(), "metadata.txt") == 0)
							RaiseErrorEvent("CreateTar", "Skipping metadata.txt file at '" + file.AbsoluteLocalPath + "' due to name conflict with the MyEmsl metadata.txt file");
						else
						{
							fileDict.Add(file.RelativeDestinationFullPath, file.AbsoluteLocalPath);
							sourceFilePathsFull.Add(file.AbsoluteLocalPath);
						}

					}					
				}

				compressor.CompressFiles(tarFileStream, sourceFilePathsFull.ToArray());
			}

			// Wait 500 msec
			System.Threading.Thread.Sleep(500);

			// Now append the metadata file
			// To append more files, we need to close the stream, then re-open it and seek to 1024 bytes before the end of the file
			// The reason for 1024 bytes is that Seven zip writes two 512 byte blocks of zeroes to the of the .Tar to signify the end of the .tar
			using (System.IO.FileStream tarFileStream = new System.IO.FileStream(outputFilePath, System.IO.FileMode.Open, System.IO.FileAccess.Write, System.IO.FileShare.Read))
			{
				tarFileStream.Seek(-1024, System.IO.SeekOrigin.End);

				fileDict.Clear();
				fileDict.Add("metadata.txt", metadataFilePath);

				compressor.CompressFileDictionary(fileDict, tarFileStream);
			}

		}

	}

}