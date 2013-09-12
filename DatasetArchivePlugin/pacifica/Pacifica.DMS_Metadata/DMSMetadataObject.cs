using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Pacifica.Core;
using MyEMSLReader;

namespace Pacifica.DMS_Metadata
{
	public class DMSMetadataObject
	{
		private Dictionary<string, object> _metadataObject;

		// List of new or changed files
		private List<FileInfoObject> _unmatchedFilesToUpload;

		private string _serverSearchString = string.Empty;

		public enum ArchiveModes
		{
			archive, update
		}

		#region "Properties"

		public Dictionary<string, object> MetadataObject
		{
			get { return this._metadataObject; }
			private set
			{
				this._metadataObject = value;
			}
		}

		public long TotalFileSizeToUpload
		{
			get;
			set;
		}

		public int TotalFileCountNew
		{
			get;
			set;
		}

		public int TotalFileCountUpdated
		{
			get;
			set;
		}

		public List<FileInfoObject> UnmatchedFilesToUpload
		{
			get { return this._unmatchedFilesToUpload; }
			private set
			{
				this._unmatchedFilesToUpload = value;
			}
		}

		public string MetadataObjectJSON
		{
			get
			{
				return Utilities.ObjectToJson(this._metadataObject);
			}
		}

		#endregion

		/// <summary>
		/// Constructor
		/// </summary>
		public DMSMetadataObject()
		{ }

		public void SetupMetadata(Dictionary<string, string> taskParams, Dictionary<string, string> mgrParams)
		{

			string datasetName;
			string datasetInstrument;
			int datasetID;
			string subFolder;

			List<FileInfoObject> lstDatasetFilesToArchive = FindDatasetFilesToArchive(taskParams, mgrParams, out datasetName, out datasetInstrument, out datasetID, out subFolder);

			// Calculate the "year_quarter" code used for subfolders within an instrument folder
			// This value is based on the date the dataset was created in DMS
			string datasetDateCodeString = GetDatasetYearQuarter(taskParams);

			// Keys in this object are key names; values are either strings or dictionary objects or even a list of dictionary objects
			var metadataObject = new Dictionary<string, object>();
			var groupObject = new List<Dictionary<string, string>>();

			// Set up the MyEMSL tagging information

			groupObject.Add(new Dictionary<string, string>() {
				{ "name", datasetInstrument }, { "type", "omics.dms.instrument" }
			});
			groupObject.Add(new Dictionary<string, string>() {
				{ "name", datasetDateCodeString }, { "type", "omics.dms.date_code" }
			});
			groupObject.Add(new Dictionary<string, string>() {
				{ "name", datasetName }, { "type", "omics.dms.dataset" }
			});
			groupObject.Add(new Dictionary<string, string>() {
				{ "name", datasetID.ToString() }, { "type", "omics.dms.dataset_id" }
			});

			var eusInfo = new Dictionary<string, object>();

			eusInfo.Add("groups", groupObject);

			string eusInstrumentID = Utilities.GetDictionaryValue(taskParams, "EUS_Instrument_ID", "");
			if (string.IsNullOrWhiteSpace(eusInstrumentID))
			{
				// This instrument does not have an EUS_Instrument_ID
				// Use 34127, which is VOrbiETD04
				eusInfo.Add("instrumentId", "34127");
			}
			else
			{
				eusInfo.Add("instrumentId", eusInstrumentID);
			}

			eusInfo.Add("instrumentName", datasetInstrument);

			string eusProposalID = Utilities.GetDictionaryValue(taskParams, "EUS_Proposal_ID", "");
			if (string.IsNullOrWhiteSpace(eusProposalID))
			{
				// This dataset does not have an EUS_Proposal_ID
				// Use 17797, which is "Development of High Throughput Proteomic Production Operations"
				eusInfo.Add("proposalID", "17797");
			}
			else
			{
				eusInfo.Add("proposalID", eusProposalID);
			}

			metadataObject.Add("bundleName", "omics_dms");
			metadataObject.Add("creationDate", Pacifica.Core.ExtensionMethods.ToUnixTime(DateTime.UtcNow).ToString());
			metadataObject.Add("eusInfo", eusInfo);

			// Find the files that are new or need to be updated
			var lstUnmatchedFiles = this.CompareDatasetContentsElasticSearch(lstDatasetFilesToArchive, subFolder, datasetID);

			metadataObject.Add("file", lstUnmatchedFiles);

			metadataObject.Add("version", "1.2.0");

			this._metadataObject = metadataObject;
			this._unmatchedFilesToUpload = lstUnmatchedFiles;

		}

		/// <summary>
		/// Find all of the files in the path to be archived
		/// </summary>
		/// <param name="pathToBeArchived">Folder path to be archived</param>
		/// <param name="baseDSPath">Base dataset folder path</param>
		/// <param name="recurse">True to recurse</param>
		/// <param name="worker">Background worker for reporting progress (can be null)</param>
		/// <returns></returns>
		private List<FileInfoObject> CollectFileInformation(
			string pathToBeArchived,
			string baseDSPath,
			bool recurse
		)
		{
			var fileCollection = new List<FileInfoObject>();

			DirectoryInfo archiveDir = new DirectoryInfo(pathToBeArchived);
			if (!archiveDir.Exists)
			{
				throw new DirectoryNotFoundException("Source directory not found: " + archiveDir);
			}

			SearchOption eSearchOption;
			if (recurse)
				eSearchOption = SearchOption.AllDirectories;
			else
				eSearchOption = SearchOption.TopDirectoryOnly;

			List<FileInfo> fileList = archiveDir.GetFiles("*.*", eSearchOption).ToList<FileInfo>();
			FileInfoObject fio;

			double fracCompleted = 0.0;
			int fileCount = fileList.Count;

			// Generate file size sum for status purposes
			long totalFileSize = 0;				// how much data is there to crunch?
			long runningFileSize = 0;			// how much data we've crunched so far
			foreach (FileInfo fi in fileList)
			{
				totalFileSize += fi.Length;
			}

			foreach (FileInfo fi in fileList)
			{
				runningFileSize += fi.Length;

				if (totalFileSize > 0)
					fracCompleted = ((double)runningFileSize / (double)totalFileSize);

				ReportProgress(fracCompleted * 100.0, "Hashing files: " + fi.Name);

				// This constructor will auto-compute the Sha-1 hash value for the file
				fio = new FileInfoObject(fi.FullName, baseDSPath);
				fileCollection.Add(fio);
			}

			ReportProgress(100);

			return fileCollection;
		}


		/// <summary>
		/// Query server for files and hash codes
		/// </summary>
		/// <param name="fileList">List of local files</param>
		/// <param name="subFolder">Optional subfolder to limit the search to (can be empty)</param>
		/// <returns></returns>
		private List<FileInfoObject> CompareDatasetContentsElasticSearch(
			List<FileInfoObject> fileList,
			string subFolder,
			int datasetID)
		{

			TotalFileSizeToUpload = 0;

			// Find all files in MyEMSL for this dataset
			var reader = new MyEMSLReader.Reader();
			List<ArchivedFileInfo> lstFilesInMyEMSL;

			lstFilesInMyEMSL = reader.FindFilesByDatasetID(datasetID, subFolder);

			if (lstFilesInMyEMSL.Count == 0)
			{
				// This dataset doesn't have any files in MyEMSL; upload everything in fileList
				foreach (var localFile in fileList)
				{
					TotalFileSizeToUpload += localFile.FileSizeInBytes;
				}
				TotalFileCountNew = fileList.Count;
				TotalFileCountUpdated = 0;

				return fileList;
			}

			// Keys in this dictionary are relative file paths
			// Values are the sha-1 hash values for the file
			Dictionary<string, string> dctFilesInMyEMSLSha1Hash = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);

			foreach (var archiveFile in lstFilesInMyEMSL)
			{
				dctFilesInMyEMSLSha1Hash.Add(archiveFile.RelativePathUnix, archiveFile.Sha1Hash);
			}

			var lstUnmatchedFiles = new List<FileInfoObject>();

			foreach (var localFile in fileList)
			{
				string itemAddress;
				if (localFile.RelativeDestinationDirectory != string.Empty)
				{
					itemAddress = localFile.RelativeDestinationDirectory + "/" + localFile.FileName;
				}
				else
				{
					itemAddress = localFile.FileName;
				}

				string fileHashMyEMSL = Utilities.GetDictionaryValue(dctFilesInMyEMSLSha1Hash, itemAddress, string.Empty);

				if (localFile.Sha1HashHex != fileHashMyEMSL)
				{
					lstUnmatchedFiles.Add(localFile);
					TotalFileSizeToUpload += localFile.FileSizeInBytes;
					if (string.IsNullOrEmpty(fileHashMyEMSL))
					{
						TotalFileCountNew++;
					}
					else
					{
						TotalFileCountUpdated++;
					}
				}
			}

			return lstUnmatchedFiles;
		}


		public List<FileInfoObject> FindDatasetFilesToArchive(
			Dictionary<string, string> taskParams, 
			Dictionary<string, string> mgrParams, 
			out string datasetName, 
			out string datasetInstrument, 
			out int datasetID, 
			out string subFolder)
		{
			//translate values from task/mgr params into usable variables
			string perspective = Utilities.GetDictionaryValue(mgrParams, "perspective", "client");
			string driveLocation;

			// Determine the drive location based on perspective 
			// (client perspective means running on a Proto storage server; server perspective means running on another computer)
			if (perspective == "client")
				driveLocation = Utilities.GetDictionaryValue(taskParams, "Storage_Vol_External", "");
			else
				driveLocation = Utilities.GetDictionaryValue(taskParams, "Storage_Vol", "");

			// Construct the dataset folder path
			string pathToArchive = Utilities.GetDictionaryValue(taskParams, "Folder", "");
			pathToArchive = Path.Combine(Utilities.GetDictionaryValue(taskParams, "Storage_Path", ""), pathToArchive);
			pathToArchive = Path.Combine(driveLocation, pathToArchive);

			datasetName = Utilities.GetDictionaryValue(taskParams, "Dataset", "");
			datasetInstrument = Utilities.GetDictionaryValue(taskParams, "Instrument_Name", "");
			datasetID = Utilities.ToIntSafe(Utilities.GetDictionaryValue(taskParams, "Dataset_ID", ""), 0);
			string baseDSPath = pathToArchive;
			subFolder = string.Empty;

			ArchiveModes archiveMode;
			if (Utilities.GetDictionaryValue(taskParams, "StepTool", "").ToLower() == "datasetarchive")
				archiveMode = ArchiveModes.archive;
			else
				archiveMode = ArchiveModes.update;

			if (archiveMode == ArchiveModes.update)
			{
				subFolder = Utilities.GetDictionaryValue(taskParams, "OutputFolderName", "").ToString();

				if (!string.IsNullOrWhiteSpace(subFolder))
					pathToArchive = Path.Combine(pathToArchive, subFolder);
				else
					subFolder = string.Empty;
			}

			bool recurse = true;
			string sValue;

			if (taskParams.TryGetValue(MyEMSLUploader.RECURSIVE_UPLOAD, out sValue))
			{
				bool.TryParse(sValue, out recurse);
			}

			// Grab file information from this dataset directory
			// This process will also compute the Sha-1 hash value for each file
			return CollectFileInformation(pathToArchive, baseDSPath, recurse);			
		}

		public static string GetDatasetYearQuarter(Dictionary<string, string> taskParams)
		{
			string datasetDate = Utilities.GetDictionaryValue(taskParams, "Created", "");
			DateTime date_code = DateTime.Parse(datasetDate);
			double yq = (double)date_code.Month / 12.0 * 4.0;
			int yearQuarter = (int)Math.Ceiling(yq);
			string datasetDateCodeString = date_code.Year.ToString() + "_" + yearQuarter.ToString();

			return datasetDateCodeString;
		}

		protected void ReportProgress(double percentComplete)
		{
			ReportProgress(percentComplete, string.Empty);
		}

		protected void ReportProgress(double percentComplete, string currentTask)
		{
			OnProgressUpdate(new Pacifica.Core.ProgressEventArgs(percentComplete, currentTask));
		}


		#region "Event Delegates and Classes"

		public event ProgressEventHandler ProgressEvent;

		public delegate void ProgressEventHandler(object sender, Pacifica.Core.ProgressEventArgs e);

		#endregion

		#region "Event Functions"

		public void OnProgressUpdate(Pacifica.Core.ProgressEventArgs e)
		{
			if (ProgressEvent != null)
				ProgressEvent(this, e);
		}
		#endregion
	}

}