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
		// Maximum number of files (per dataset) to archive
		public const int MAX_FILES_TO_ARCHIVE = 1000;

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
			Upload.udtUploadMetadata uploadMetadata;

			uploadMetadata.EUSInstrumentID = Utilities.GetDictionaryValue(taskParams, "EUS_Instrument_ID", "");
			uploadMetadata.EUSProposalID = Utilities.GetDictionaryValue(taskParams, "EUS_Proposal_ID", "");
			
			List<FileInfoObject> lstDatasetFilesToArchive = FindDatasetFilesToArchive(taskParams, mgrParams, out uploadMetadata);

			// Calculate the "year_quarter" code used for subfolders within an instrument folder
			// This value is based on the date the dataset was created in DMS
			uploadMetadata.DateCodeString = GetDatasetYearQuarter(taskParams);

			// Find the files that are new or need to be updated
			List<FileInfoObject> lstUnmatchedFiles = this.CompareDatasetContentsElasticSearch(lstDatasetFilesToArchive, uploadMetadata);

			Dictionary<string, object> metadataObject = Upload.CreateMetadataObject(uploadMetadata, lstUnmatchedFiles);

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

			if (fileList.Count >= MAX_FILES_TO_ARCHIVE)
			{
				throw new ArgumentOutOfRangeException("Source directory has over " + MAX_FILES_TO_ARCHIVE + " files; files must be zipped before upload to MyEMSL");
			}

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
			Upload.udtUploadMetadata uploadMetadata)
		{

			TotalFileSizeToUpload = 0;

			// Find all files in MyEMSL for this dataset
			var reader = new MyEMSLReader.Reader();
			reader.IncludeAllRevisions = false;

			// Attach events
			reader.ErrorEvent += new MyEMSLReader.MessageEventHandler(reader_ErrorEvent);
			reader.MessageEvent += new MyEMSLReader.MessageEventHandler(reader_MessageEvent);
			reader.ProgressEvent += new MyEMSLReader.ProgressEventHandler(reader_ProgressEvent);

			List<ArchivedFileInfo> lstFilesInMyEMSL;

			lstFilesInMyEMSL = reader.FindFilesByDatasetID(uploadMetadata.DatasetID, uploadMetadata.SubFolder);

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
			out Upload.udtUploadMetadata uploadMetadata)
		{

			uploadMetadata = new Upload.udtUploadMetadata();
			uploadMetadata.Clear();			
			
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

			uploadMetadata.DatasetName = Utilities.GetDictionaryValue(taskParams, "Dataset", "");
			uploadMetadata.DMSInstrumentName = Utilities.GetDictionaryValue(taskParams, "Instrument_Name", "");
			uploadMetadata.DatasetID = Utilities.ToIntSafe(Utilities.GetDictionaryValue(taskParams, "Dataset_ID", ""), 0);
			string baseDSPath = pathToArchive;
			uploadMetadata.SubFolder = string.Empty;

			ArchiveModes archiveMode;
			if (Utilities.GetDictionaryValue(taskParams, "StepTool", "").ToLower() == "datasetarchive")
				archiveMode = ArchiveModes.archive;
			else
				archiveMode = ArchiveModes.update;

			if (archiveMode == ArchiveModes.update)
			{
				uploadMetadata.SubFolder = Utilities.GetDictionaryValue(taskParams, "OutputFolderName", "").ToString();

				if (!string.IsNullOrWhiteSpace(uploadMetadata.SubFolder))
					pathToArchive = Path.Combine(pathToArchive, uploadMetadata.SubFolder);
				else
					uploadMetadata.SubFolder = string.Empty;
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

		void reader_ErrorEvent(object sender, MyEMSLReader.MessageEventArgs e)
		{
			Console.WriteLine("Error in MyEMSLReader: " + e.Message);
		}

		void reader_MessageEvent(object sender, MyEMSLReader.MessageEventArgs e)
		{
			// Console.WriteLine("MyEMSLReader: " + e.Message);
		}

		void reader_ProgressEvent(object sender, MyEMSLReader.ProgressEventArgs e)
		{
			// Console.WriteLine("MyEMSLReader Percent complete: " + e.PercentComplete.ToString("0.0") + "%");
		}

		#endregion
	}

}