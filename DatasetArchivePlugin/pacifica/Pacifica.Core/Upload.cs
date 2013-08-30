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

		private CookieContainer mCookieJar;

		#endregion

		#region Constructor

		public Upload()
		{

			/* August 2013: To be deleted
			 *
			 * backgrounder.WorkerReportsProgress = true;
			 * backgrounder.WorkerSupportsCancellation = true;
			 * this._topLevelBackgrounder = backgrounder;
			 * statusBackgrounder = new BackgroundWorker();
			*/

			// Note that EasyHttp is a static class with a static event
			// Be careful about instantiating this class (Upload) multiple times
			EasyHttp.StatusUpdate += new StatusUpdateEventHandler(EasyHttp_StatusUpdate);

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

		/* August 2013: To be deleted
		 *
		public void RaiseStatusUpdate(string bundleIdentifier,
				double percentCompleted, long totalBytesSent,
				long totalBytesToSend, string statusMessage)
		{
			if (StatusUpdate != null)
			{
				StatusUpdate(this, new StatusEventArgs(bundleIdentifier, percentCompleted, totalBytesSent, totalBytesToSend, statusMessage));
			}
		}
		*/

		private void RaiseUploadCompleted(string bundleIdentifier, string serverResponse)
		{
			if (UploadCompleted != null)
			{
				UploadCompleted(this, new UploadCompletedEventArgs(bundleIdentifier, serverResponse));
			}
		}

		#endregion

		#region IUpload Members

		public bool StartUpload(IDictionary metadataObject, out string statusURL)
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
		public bool StartUpload(IDictionary metadataObject, NetworkCredential loginCredentials, out string statusURL)
		{
			string timestamp = DateTime.Now.ToString("yyyyMMddHHmmssffffff");
			statusURL = string.Empty;

			string bundleName = timestamp + bundleExtension;
			if (metadataObject.Contains("bundleName") && metadataObject["bundleName"] is string)
			{
				metadataObject["bundleName"] += "_" + bundleName;
			}
			else
			{
				metadataObject.Add("bundleName", bundleName);
			}

			var fileList = (List<Pacifica.Core.FileInfoObject>)metadataObject["file"];

			if (fileList.Count == 0)
			{
				RaiseDebugEvent("ProcessMetadata", "File list is empty; nothing to do");
				RaiseUploadCompleted(bundleName, "");
				return true;
			}

			// Grab the list of files from the top-level "file" object
			// Keys in this dictionary are the source file path; values are metadata about the file
			SortedDictionary<string, FileInfoObject> fileListObject = new SortedDictionary<string, FileInfoObject>();

			// This is a list of dictionary objects
			// Dictionary keys will be sha1Hash, destinationDirectory, and fileName
			var newFileObj = new List<Dictionary<string, string>>();

			foreach (var file in fileList)
			{

				FileInfoObject fiObj = new FileInfoObject(file.AbsoluteLocalPath, file.RelativeDestinationDirectory, file.Sha1HashHex);

				if (fiObj.RelativeDestinationFullPath.ToLower() == "metadata.txt")
				{
					// We must skip this file since MyEMSL stores a special metadata.txt file at the root of the .tar file
					// The alternative would be to create a tar file with all of the data files (and folders) in a subfolder named data
					// In this case we would define the data as version 1.2 instead of 1.0
					//   metadataObject.Add("version", "1.2");
					// However, creating a .tar file with the data in this layout is tricky with 7-zip, so we'll just skip metadata.txt files, which really shouldn't hurt anything

					RaiseErrorEvent("ProcessMetadata", "Skipping metadata.txt file at '" + fiObj.AbsoluteLocalPath + "' due to name conflict with the MyEmsl metadata.txt file");
				}
				else
				{
					fileListObject.Add(file.AbsoluteLocalPath, fiObj);
					newFileObj.Add(fiObj.SerializeToDictionaryObject());
				}
			}

			RaiseDebugEvent("ProcessMetadata", "Bundling " + newFileObj.Count + " files");
			metadataObject["file"] = newFileObj;

			string mdJson = Utilities.ObjectToJson(metadataObject);

			// Create the metadata.txt file
			string metadataFilename = Path.GetTempFileName();
			FileInfo mdTextFile = new FileInfo(metadataFilename);
			using (StreamWriter sw = mdTextFile.CreateText())
			{
				sw.Write(mdJson);
			}

			string bundleNameFull = metadataObject["bundleName"].ToString();

			/* August 2013: To be deleted
			 *
			 * FileInfo fiTarFile = BundleFiles(fileListObject, mdTextFile.FullName, bundleNameFull);
			 */

			NetworkCredential newCred = null;
			if (loginCredentials != null)
			{
				newCred = new NetworkCredential(loginCredentials.UserName,
						loginCredentials.Password, loginCredentials.Domain);
			}

			// Call the testauth service to obtain a cookie for this session
			string authURL = Pacifica.Core.Configuration.TestAuthUri;
			Auth auth = new Auth(new Uri(authURL));

			mCookieJar = null;
			if (!auth.GetAuthCookies(out mCookieJar))
			{
				string msg = "Auto-login to " + Pacifica.Core.Configuration.TestAuthUri + " failed authentication";
				RaiseErrorEvent("ProcessMetadata", msg);
				throw new ApplicationException(msg);
			}

			string redirectedServer = Pacifica.Core.Configuration.ServerUri;
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
				RaiseUploadCompleted(bundleName, finishResult);
				success = false;
			}
			else if (m.Groups["accepted"].Success && m.Groups["url"].Success)
			{
				statusURL = m.Groups["url"].Value.Trim();
				RaiseUploadCompleted(bundleName, statusURL);
				success = true;
			}
			else
			{
				throw new ApplicationException(finishUrl + " failed with message: " + finishResult);
			}

			try
			{
				mdTextFile.Delete();
			}
			catch
			{
				// Ignore errors here
			}

			return success;
		}

		public string GenerateSha1Hash(string fullFilePath)
		{
			return Utilities.GenerateSha1Hash(fullFilePath);
		}

		#endregion

		#region  Member Methods

		/* August 2013: To be deleted
		 *
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
			double percentComplete = 0;

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
			var sourceFilePathsFull = new System.Collections.Generic.List<string>();

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

		 */

		#endregion

	}

}