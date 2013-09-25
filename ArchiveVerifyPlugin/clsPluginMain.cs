using System;
using CaptureTaskManager;
using Pacifica.Core;
using Pacifica.DMS_Metadata;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.IO;

namespace ArchiveVerifyPlugin
{
	public class clsPluginMain : clsToolRunnerBase
	{
		//*********************************************************************************************************
		// Main class for plugin
		//**********************************************************************************************************

		#region "Constants and Enums"
		protected const string MD5_RESULTS_FILE_PREFIX = "results.";
		protected const string DEFAULT_MD5_RESULTS_FOLDER_PATH = @"\\proto-7\MD5Results";
		protected const string DEFAULT_MD5_RESULTS_BACKUP_FOLDER_PATH = @"\\proto-3\MD5ResultsBackup";

		#endregion

		#region "Class-wide variables"
		clsToolReturnData mRetData = new clsToolReturnData();

		protected double mPercentComplete;
		protected DateTime mLastProgressUpdateTime = DateTime.UtcNow;

		#endregion

		#region "Constructors"
		public clsPluginMain()
			: base()
		{

		}

		#endregion

		#region "Methods"
		/// <summary>
		/// Runs the Archive Verify step tool
		/// </summary>
		/// <returns>Class with completionCode, completionMessage, evaluationCode, and evaluationMessage</returns>
		public override clsToolReturnData RunTool()
		{
			string msg;

			msg = "Starting ArchiveVerifyPlugin.clsPluginMain.RunTool()";
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

			// Perform base class operations, if any
			mRetData = base.RunTool();
			if (mRetData.CloseoutType == EnumCloseOutType.CLOSEOUT_FAILED)
				return mRetData;

			if (m_DebugLevel >= 5)
			{
				msg = "Verifying files in MyEMSL for dataset '" + m_Dataset + "'";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
			}

			// Set this to Success for now
			mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;

			// Examine the MyEMSL ingest status page
			bool success = CheckUploadStatus();

			if (success)
			{
				string metadataFilePath;

				// Confirm that the files are visible in elastic search
				// If data is found, then CreateOrUpdateMD5ResultsFile will also be called
				success = VisibleInElasticSearch(out metadataFilePath);
				
				if (success && !string.IsNullOrEmpty(metadataFilePath))
				{
					DeleteMetadataFile(metadataFilePath);
				}
			}

			if (success)
			{
				// Everything was good
				if (m_DebugLevel >= 4)
				{
					msg = "MyEMSL verification successful for dataset " + m_Dataset;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
				}
				mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;

				// Note that stored procedure SetStepTaskComplete will update MyEMSL State values if mRetData.EvalCode = 5
				mRetData.EvalCode = EnumEvalCode.EVAL_CODE_VERIFIED_IN_MYEMSL;
					
			}
			else
			{
				// There was a problem	
				if (mRetData.CloseoutType == EnumCloseOutType.CLOSEOUT_SUCCESS)
					mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_NOT_READY;
			}

			msg = "Completed clsPluginMain.RunTool()";
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

			return mRetData;

		}	// End sub

		protected bool CheckUploadStatus()
		{

			// Monitor the upload status for 60 seconds
			// If still not complete after 60 seconds, then this manager will return completionCode CLOSEOUT_NOT_READY=2 
			// which will tell the DMS_Capture DB to reset the task to state 2 and bump up the Next_Try value by 5 minutes
			const int MAX_WAIT_TIME_SECONDS = 60;

			DateTime dtStartTime = DateTime.UtcNow;

			string statusURI = m_TaskParams.GetParam("MyEMSL_Status_URI", "");

			if (string.IsNullOrEmpty(statusURI))
			{
				string msg = "MyEMSL_Status_URI is empty; cannot verify upload status";
				mRetData.CloseoutMsg = msg;
				mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
				return false;
			}

			// Call the testauth service to obtain a cookie for this session
			string authURL = Configuration.TestAuthUri;
			Auth auth = new Auth(new Uri(authURL));

			CookieContainer cookieJar = null;
			if (!auth.GetAuthCookies(out cookieJar))
			{
				string msg = "Auto-login to " + Configuration.TestAuthUri + " failed authentication";
				mRetData.CloseoutMsg = "Failed to obtain MyEMSL session cookie";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
				return false;
			}

			int exceptionCount = 0;
			var statusChecker = new MyEMSLStatusCheck();

			while (DateTime.UtcNow.Subtract(dtStartTime).TotalSeconds < MAX_WAIT_TIME_SECONDS)
			{

				try
				{

					bool accessDenied;
					string statusMessage;

					if (statusChecker.IngestStepCompleted(statusURI, MyEMSLStatusCheck.StatusStep.Available, cookieJar, out accessDenied, out statusMessage))
					{
						Utilities.Logout(cookieJar);
						return true;
					}

					if (accessDenied)
					{
						mRetData.CloseoutMsg = statusMessage;
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, statusMessage);
						Utilities.Logout(cookieJar);
						return false;
					}

				}
				catch (Exception ex)
				{
					exceptionCount++;
					if (exceptionCount < 3)
					{
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception checking upload status: " + ex.Message);
					}
					else
					{
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception checking upload status: ", ex);
						break;
					}
				}

				exceptionCount = 0;

				// Sleep for 5 seconds
				System.Threading.Thread.Sleep(5000);

			}

			Utilities.Logout(cookieJar);

			return false;
		}


		/// <summary>
		/// Compare the files in lstArchivedFiles to the files in the metadata.txt file
		/// If metadata.txt file is missing, then compare to files actually on disk
		/// </summary>
		/// <param name="lstArchivedFiles"></param>
		/// <returns></returns>
		protected bool CompareArchiveFilesToExpectedFiles(List<MyEMSLReader.ArchivedFileInfo> lstArchivedFiles, out string metadataFilePath)
		{
			metadataFilePath = string.Empty;

			try
			{

				int matchCount;
				int mismatchCount;

				string transferFolderPath = m_TaskParams.GetParam("TransferFolderPath", "");
				if (!ParameterDefined("TransferFolderPath", transferFolderPath))
					return false;

				string datasetName = m_TaskParams.GetParam("Dataset", "");
				if (!ParameterDefined("Dataset", datasetName))
					return false;

				transferFolderPath = Path.Combine(transferFolderPath, datasetName);

				string jobNumber = m_TaskParams.GetParam("Job", "");
				if (!ParameterDefined("Job", jobNumber))
					return false;

				var fiMetadataFile = new FileInfo(Path.Combine(transferFolderPath, Utilities.GetMetadataFilenameForJob(jobNumber)));

				if (fiMetadataFile.Exists)
				{
					metadataFilePath = fiMetadataFile.FullName;

					CompareToMetadataFile(lstArchivedFiles, fiMetadataFile, out matchCount, out mismatchCount);

					if (matchCount > 0 && mismatchCount == 0)
					{
						// Everything matches up
						return true;
					}

					if (mismatchCount > 0)
					{
						string matchStats = "MatchCount=" + matchCount + ", MismatchCount=" + mismatchCount;
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Sha1 mismatch between local files in metadata.txt file and MyEMSL; " + matchStats);
						mRetData.CloseoutMsg = matchStats;
						return false;
					}
				}

				// Metadata file was missing or empty; compare to local files on disk

				// Look for files that should have been uploaded, compute a Sha-1 hash for each, and compare those hashes to existing files in MyEMSL

				string datasetInstrument;
				int datasetID;
				string subFolder;

				var oDatasetFileMetadata = new DMSMetadataObject();
				List<FileInfoObject> lstDatasetFilesLocal = oDatasetFileMetadata.FindDatasetFilesToArchive(m_TaskParams.TaskDictionary, m_MgrParams.TaskDictionary, out datasetName, out datasetInstrument, out datasetID, out subFolder);

				if (lstDatasetFilesLocal.Count == 0)
				{
					mRetData.CloseoutMsg = "Local files were not found for this dataset; unable to compare Sha-1 hashes to MyEMSL values";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.CloseoutMsg);
					return false;
				}

				// Keys are relative file paths (Windows slashes)
				// Values are the sha-1 hash values
				var dctFilePathHashMap = new Dictionary<string, string>();

				foreach (var datasetFile in lstDatasetFilesLocal)
				{
					string relativeFilePathWindows = datasetFile.RelativeDestinationFullPath.Replace("/", @"\");
					dctFilePathHashMap.Add(relativeFilePathWindows, datasetFile.Sha1HashHex);
				}

				CompareArchiveFilesToList(lstArchivedFiles, out matchCount, out mismatchCount, dctFilePathHashMap);

				if (matchCount > 0 && mismatchCount == 0)
				{
					// Everything matches up
					return true;
				}

				if (mismatchCount > 0)
				{
					mRetData.CloseoutMsg = "Sha1 mismatch between local files on disk and MyEMSL; MatchCount=" + matchCount + ", MismatchCount=" + mismatchCount;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.CloseoutMsg);
					return false;
				}

				return false;

			}
			catch (Exception ex)
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception in CompareArchiveFilesToExpectedFiles", ex);
				return false;
			}

		}

		protected void CompareArchiveFilesToList(List<MyEMSLReader.ArchivedFileInfo> lstArchivedFiles, out int matchCount, out int mismatchCount, Dictionary<string, string> dctFilePathHashMap)
		{
			matchCount = 0;
			mismatchCount = 0;

			// Make sure each of the files in dctFilePathHashMap is present in lstArchivedFiles
			foreach (var metadataFile in dctFilePathHashMap)
			{
				var lstMatchingArchivedFiles = (from item in lstArchivedFiles where item.RelativePathWindows == metadataFile.Key select item).ToList();

				if (lstMatchingArchivedFiles.Count == 0)
				{
					string msg = "File " + metadataFile.Key + " not found in MyEMSL (CompareArchiveFilesToList)";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
				}
				else
				{
					var archiveFile = lstMatchingArchivedFiles.First();

					if (archiveFile.Sha1Hash == metadataFile.Value)
						matchCount++;
					else
					{
						string msg = "File mismatch for " + archiveFile.RelativePathWindows + "; MyEMSL reports " + archiveFile.Sha1Hash + " but expecting " + metadataFile.Value;
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);

						mismatchCount++;
					}
				}
			}
		}

		protected void CompareToMetadataFile(List<MyEMSLReader.ArchivedFileInfo> lstArchivedFiles, FileInfo fiMetadataFile, out int matchCount, out int mismatchCount)
		{
			matchCount = 0;
			mismatchCount = 0;

			// Parse the contents of the file
			string contents = string.Empty;

			using (var srMetadataFile = new StreamReader(new FileStream(fiMetadataFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read)))
			{
				contents = srMetadataFile.ReadToEnd();
			}

			if (string.IsNullOrEmpty(contents))
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Metadata file is empty: " + fiMetadataFile.FullName);
				return;
			}

			var dctMetadataInfo = Utilities.JsonToObject(contents);
			if (!dctMetadataInfo.ContainsKey("file"))
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Metadata file JSON does not contain 'file': " + fiMetadataFile.FullName);
				return;
			}

			var dctMetadataFiles = (List<Dictionary<string, object>>)dctMetadataInfo["file"];

			// Keys are relative file paths (Windows slashes)
			// Values are the sha-1 hash values
			var dctFilePathHashMap = new Dictionary<string, string>();

			foreach (var metadataFile in dctMetadataFiles)
			{
				string sha1Hash = metadataFile["sha1Hash"].ToString();
				string destinationDirectory = metadataFile["destinationDirectory"].ToString();
				string fileName = metadataFile["fileName"].ToString();

				string relativeFilePathWindows = Path.Combine(destinationDirectory.Replace('/', Path.DirectorySeparatorChar), fileName);

				dctFilePathHashMap.Add(relativeFilePathWindows, sha1Hash);
			}

			CompareArchiveFilesToList(lstArchivedFiles, out matchCount, out mismatchCount, dctFilePathHashMap);

		}

		protected void CopyMD5ResultsFileToBackupFolder(string datasetInstrument, string datasetYearQuarter, FileInfo fiMD5ResultsFile)
		{
			// Copy the updated file to md5ResultsFolderPathBackup
			try
			{
				string md5ResultsFolderPathBackup = DEFAULT_MD5_RESULTS_BACKUP_FOLDER_PATH;

				string strMD5ResultsFileBackup = GetMD5ResultsFilePath(md5ResultsFolderPathBackup, m_Dataset, datasetInstrument, datasetYearQuarter);

				var fiBackupMD5ResultsFile = new FileInfo(strMD5ResultsFileBackup);

				// Create the target folders if necessary
				if (!fiBackupMD5ResultsFile.Directory.Exists)
					fiBackupMD5ResultsFile.Create();

				// Copy the file
				fiMD5ResultsFile.CopyTo(fiBackupMD5ResultsFile.FullName, true);

			}
			catch (Exception ex)
			{
				// Don't treat this as a fatal error
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error copying MD5 results file from MD5 results folder to backup folder", ex);
			}
		}

		protected bool CreateMD5ResultsFile(FileInfo fiMD5ResultsFile, List<MyEMSLReader.ArchivedFileInfo> lstArchivedFiles)
		{
			// Create a new MD5 results file
			bool success = false;

			try
			{
				// Create a new MD5 results file
				var lstMD5Results = new Dictionary<string, clsHashInfo>(StringComparer.CurrentCultureIgnoreCase);

				foreach (var archivedFile in lstArchivedFiles)
				{
					string archivedFilePath = "/myemsl/svc-dms/" + archivedFile.PathWithInstrumentAndDatasetUnix;

					var hashInfo = new clsHashInfo();
					hashInfo.HashCode = archivedFile.Sha1Hash;
					hashInfo.MyEMSLFileID = archivedFile.FileID.ToString();

					lstMD5Results.Add(archivedFilePath, hashInfo);
				}

				// Create the target folders if necessary
				if (!fiMD5ResultsFile.Directory.Exists)
					fiMD5ResultsFile.Directory.Create();

				success = WriteMD5ResultsFile(lstMD5Results, fiMD5ResultsFile.FullName, useTempFile: false);

			}
			catch (Exception ex)
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception creating new MD5 results file in CreateMD5ResultsFile", ex);
				return false;
			}

			return success;

		}

		/// <summary>
		/// Create or update the MD5 results file
		/// </summary>
		/// <param name="lstArchivedFiles"></param>
		/// <returns></returns>
		protected bool CreateOrUpdateMD5ResultsFile(List<MyEMSLReader.ArchivedFileInfo> lstArchivedFiles)
		{

			bool success = false;

			try
			{

				string md5ResultsFolderPathMaster = DEFAULT_MD5_RESULTS_FOLDER_PATH;

				string datasetInstrument = m_TaskParams.GetParam("Instrument_Name", "");
				if (!ParameterDefined("Instrument_Name", datasetInstrument))
					return false;

				// Calculate the "year_quarter" code used for subfolders within an instrument folder
				// This value is based on the date the dataset was created in DMS
				string datasetYearQuarter = DMSMetadataObject.GetDatasetYearQuarter(m_TaskParams.TaskDictionary);

				string md5ResultsFilePath = GetMD5ResultsFilePath(md5ResultsFolderPathMaster, m_Dataset, datasetInstrument, datasetYearQuarter);

				var fiMD5ResultsFile = new FileInfo(md5ResultsFilePath);

				if (!fiMD5ResultsFile.Exists)
				{
					// Target file doesn't exist; nothing to merge
					success = CreateMD5ResultsFile(fiMD5ResultsFile, lstArchivedFiles);				
				}
				else
				{
					// File exists; merge the new values with the existing data
					success = UpdateMD5ResultsFile(md5ResultsFilePath, lstArchivedFiles);
				}

				if (success)
				{
					CopyMD5ResultsFileToBackupFolder(datasetInstrument, datasetYearQuarter, fiMD5ResultsFile);
				}

			}
			catch (Exception ex)
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception in CreateMD5ResultsFile", ex);
				return false;
			}

			return success;

		}

		protected void DeleteMetadataFile(string metadataFilePath)
		{
			try
			{
				var fiMetadataFile = new FileInfo(metadataFilePath);
				var diParentFolder = fiMetadataFile.Directory;

				// Delete the metadata file in the transfer folder
				if (fiMetadataFile.Exists)
					fiMetadataFile.Delete();

				// Delete the transfer folder if it is empty
				if (diParentFolder.GetFileSystemInfos("*", SearchOption.AllDirectories).Length == 0)
					diParentFolder.Delete();

			}
			catch (Exception ex)
			{
				// Don't treat this as a fatal error
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error deleting metadata file in transfer folder: " + ex.Message);
			}
		}

		protected string GetMD5ResultsFilePath(string strMD5ResultsFolderPath, string strDatasetName, string strInstrumentName, string strDatasetYearQuarter)
		{
			return GetMD5ResultsFilePath(Path.Combine(strMD5ResultsFolderPath, strInstrumentName, strDatasetYearQuarter), strDatasetName);
		}

		protected string GetMD5ResultsFilePath(string strParentFolderPath, string strDatasetName)
		{
			return Path.Combine(strParentFolderPath, MD5_RESULTS_FILE_PREFIX + strDatasetName);
		}

		protected bool ParameterDefined(string parameterName, string parameterValue)
		{
			if (string.IsNullOrEmpty(parameterValue))
			{
				mRetData.CloseoutMsg = "Job parameters do not have " + parameterName + " defined; unable to continue";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.CloseoutMsg);
				return false;
			}

			return true;
		}

		/// <summary>
		/// Parse out the hash and file path from dataLine
		/// Updates lstMD5Results (or adds a new entry)
		/// </summary>
		/// <param name="dataLine"></param>
		/// <param name="lstMD5Results">Dictionary where keys are unix file paths and values are clsHashInfo, tracking the Hash value and MyEMSL File ID</param>
		/// <returns>True if lstMD5Results is updated, false if unchanged</returns>
		/// <remarks></remarks>
		protected bool ParseAndStoreHashInfo(string dataLine, ref Dictionary<string, clsHashInfo> lstMD5Results)
		{
			// Data Line Format
			//
			// Old data not in MyEMSL:
			//    MD5Hash<SPACE>ArchiveFilePath
			//
			// New data in MyEMSL:
			//    Sha1Hash<SPACE>MyEMSLFilePath<TAB>MyEMSLID
			//
			// The Hash and ArchiveFilePath are separated by a space because that's how Ryan Wright's script reported the results
			// The FilePath and MyEMSLID are separated by a tab in case the file path contains a space

			// Examples:
			//
			// Old data not in MyEMSL:
			//    0dcf9d677ac76519ae54c11cc5e10723 /archive/dmsarch/VOrbiETD04/2013_3/QC_Shew_13_04-100ng-3_HCD_19Aug13_Frodo_13-04-15/QC_Shew_13_04-100ng-3_HCD_19Aug13_Frodo_13-04-15.raw
			//    d47aca4d13d0a771900eef1fc7ee53ce /archive/dmsarch/VOrbiETD04/2013_3/QC_Shew_13_04-100ng-3_HCD_19Aug13_Frodo_13-04-15/QC/index.html
			//
			// New data in MyEMSL:
			//    796d99bcc6f1824dfe1c36cc9a61636dd1b07625 /myemsl/svc-dms/SW_TEST_LCQ/2006_1/SWT_LCQData_300/SIC201309041722_Auto976603/Default_2008-08-22.xml	915636
			//    70976fbd7088b27a711de4ce6309fbb3739d05f9 /myemsl/svc-dms/SW_TEST_LCQ/2006_1/SWT_LCQData_300/SIC201309041722_Auto976603/SWT_LCQData_300_TIC_Scan.tic	915648

			var hashInfo = new clsHashInfo();

			var lstValues = dataLine.Split(new char[] { ' ' }, 2).ToList();

			if (lstValues.Count < 2)
			{
				// Line doesn't contain two strings separated by a space
				return false;
			}

			hashInfo.HashCode = lstValues[0];

			var lstPathInfo = lstValues[1].Split(new char[] { '\t' }).ToList();

			string archiveFilePath = lstPathInfo[0];

			if (lstPathInfo.Count > 1)
				hashInfo.MyEMSLFileID = lstPathInfo[1];

			// Files should only be listed once in the MD5 results file
			// But, just in case there is a duplicate, we'll check for that
			// Results files could have duplicate entries if a file was copied to the archive via FTP and was stored via MyEMSL
			clsHashInfo hashInfoCached;
			if (lstMD5Results.TryGetValue(archiveFilePath, out hashInfoCached))
			{
				if (hashInfo.IsMatch(hashInfoCached))
				{
					// Values match; nothing to update
					return false;
				}
				else
				{
					// Preferentially use the newer value, unless the older value has a MyEMSL file ID but the newer one does not
					if (string.IsNullOrEmpty(hashInfo.MyEMSLFileID) && !string.IsNullOrEmpty(hashInfoCached.MyEMSLFileID))
						// Do not update the dictionary
						return false;
					
					lstMD5Results[archiveFilePath] = hashInfo;					
					return true;
				}
			}
			else
			{
				// Append a new entry to the cached info
				lstMD5Results.Add(archiveFilePath, hashInfo);
				return true;
			}
		
		}

		protected bool UpdateMD5ResultsFile(string md5ResultsFilePath, List<MyEMSLReader.ArchivedFileInfo> lstArchivedFiles)
		{
			bool success = false;

			var lstMD5Results = new Dictionary<string, clsHashInfo>(StringComparer.CurrentCultureIgnoreCase);

			// Read the file and cache the results in memory
			using (var srMD5ResultsFile = new StreamReader(new FileStream(md5ResultsFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
			{
				while (srMD5ResultsFile.Peek() > -1)
				{
					string dataLine = srMD5ResultsFile.ReadLine();
					ParseAndStoreHashInfo(dataLine, ref lstMD5Results);
				}
			}

			bool saveMergedFile = false;

			// Merge the results in lstArchivedFiles with lstMD5Results
			foreach (var archivedFile in lstArchivedFiles)
			{
				string archivedFilePath = "/myemsl/svc-dms/" + archivedFile.PathWithInstrumentAndDatasetUnix;

				var hashInfo = new clsHashInfo();
				hashInfo.HashCode = archivedFile.Sha1Hash;
				hashInfo.MyEMSLFileID = archivedFile.FileID.ToString();

				clsHashInfo hashInfoCached;
				if (lstMD5Results.TryGetValue(archivedFilePath, out hashInfoCached))
				{
					if (!hashInfo.IsMatch(hashInfoCached))
					{
						lstMD5Results[archivedFilePath] = hashInfo;
						saveMergedFile = true;
					}
				}
				else
				{
					lstMD5Results.Add(archivedFilePath, hashInfo);
					saveMergedFile = true;
				}

			}

			if (saveMergedFile)
			{
				success = WriteMD5ResultsFile(lstMD5Results, md5ResultsFilePath, useTempFile: true);
			}
			else
			{
				success = true;
			}

			return success;
		}

		protected bool VisibleInElasticSearch(out string metadataFilePath)
		{
			bool success = false;

			metadataFilePath = string.Empty;

			try
			{

				var reader = new MyEMSLReader.Reader();
				reader.IncludeAllRevisions = false;

				// Attach events
				reader.ErrorEvent += new MyEMSLReader.MessageEventHandler(reader_ErrorEvent);
				reader.MessageEvent += new MyEMSLReader.MessageEventHandler(reader_MessageEvent);
				reader.ProgressEvent += new MyEMSLReader.ProgressEventHandler(reader_ProgressEvent);

				string subDir = m_TaskParams.GetParam("OutputFolderName", "");

				var lstArchivedFiles = reader.FindFilesByDatasetID(m_DatasetID, subDir);

				if (lstArchivedFiles.Count == 0)
				{
					string msg = "Elastic search did not find any files for Dataset_ID " + m_DatasetID;
					if (!string.IsNullOrEmpty(subDir))
						msg += " and subdirectory " + subDir;

					mRetData.CloseoutMsg = msg;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
					return false;
				}

				success = CompareArchiveFilesToExpectedFiles(lstArchivedFiles, out metadataFilePath);

				if (success)
				{
					success = CreateOrUpdateMD5ResultsFile(lstArchivedFiles);
				}			

			}
			catch (Exception ex)
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception in VisibleInElasticSearch", ex);
				return false;
			}

			return success;

		}
				
		protected bool WriteMD5ResultsFile(Dictionary<string, clsHashInfo> lstMD5Results, string md5ResultsFilePath, bool useTempFile)
		{
			string currentStep = "initializing";

			try
			{
				string targetFilePath;
				if (useTempFile)
				{
					// Create a new MD5 results file that we'll use to replace strMD5ResultsFileMaster
					targetFilePath = md5ResultsFilePath + ".new";
				}
				else
				{
					targetFilePath = md5ResultsFilePath;
				}

				currentStep = "creating " + targetFilePath;

				using (var swMD5ResultsFile = new StreamWriter(new FileStream(targetFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
				{
					foreach (var item in lstMD5Results)
					{
						string dataLine = item.Value.HashCode + " " + item.Key;
						if (!string.IsNullOrEmpty(item.Value.MyEMSLFileID))
							dataLine += "\t" + item.Value.MyEMSLFileID;

						swMD5ResultsFile.WriteLine(dataLine);
					}
				}

				System.Threading.Thread.Sleep(50);

				if (useTempFile)
				{
					currentStep = "overwriting master MD5 results file with " + targetFilePath;
					File.Copy(targetFilePath, md5ResultsFilePath, true);
					System.Threading.Thread.Sleep(100);

					currentStep = "deleting " + targetFilePath;
					File.Delete(targetFilePath);
				}

				return true;

			}
			catch (Exception ex)
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception in WriteMD5ResultsFile while " + currentStep, ex);
				return false;
			}

		}

		#endregion

		#region "Event Handlers"
		void reader_ErrorEvent(object sender, MyEMSLReader.MessageEventArgs e)
		{
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "MyEMSLReader: " + e.Message);
		}

		void reader_MessageEvent(object sender, MyEMSLReader.MessageEventArgs e)
		{
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, e.Message);
		}

		void reader_ProgressEvent(object sender, MyEMSLReader.ProgressEventArgs e)
		{
			string msg = "Percent complete: " + e.PercentComplete.ToString("0.0") + "%";

			if (e.PercentComplete > mPercentComplete || DateTime.UtcNow.Subtract(mLastProgressUpdateTime).TotalSeconds >= 30)
			{
				if (DateTime.UtcNow.Subtract(mLastProgressUpdateTime).TotalSeconds >= 1)
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
					mPercentComplete = e.PercentComplete;
					mLastProgressUpdateTime = DateTime.UtcNow;
				}
			}
		}

		#endregion

	}	// End class

}	// End namespace
