
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 10/20/2009
//
// Last modified 10/20/2009
//						02/03/2010 (DAC) - Modified logging to include job number
//*********************************************************************************************************
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using CaptureTaskManager;
using System.IO;
using PRISM.Files;
using System.Security.Cryptography;
using MD5StageFileCreator;

namespace DatasetArchivePlugin
{
	class clsArchiveUpdate : clsOpsBase
	{
		//*********************************************************************************************************
		// Tools to perform archive update operations
		//**********************************************************************************************************

		#region "Constants"
		const int FILE_COMPARE_EQUAL = -1;
		const int FILE_COMPARE_NOT_EQUAL = 0;
		const int FILE_COMPARE_ERROR = 1;

		#endregion

		#region "Class variables"

		string m_ArchiveSharePath = string.Empty;
		string m_ResultsFolderPathArchive = string.Empty;
		string m_ResultsFolderPathServer = string.Empty;
		string m_DatasetName = string.Empty;

		clsMD5StageFileCreator mMD5StageFileCreator;

		#endregion

		#region "Constructors"
		/// <summary>
		/// Constructor
		/// </summary>
		/// <param name="MgrParams">Manager parameters</param>
		/// <param name="TaskParasms">Task parameters</param>
		public clsArchiveUpdate(IMgrParams MgrParams, ITaskParams TaskParams)
			: base(MgrParams, TaskParams)
		{
		}	// End sub
		#endregion

		#region "Methods"
		/// <summary>
		/// Performs an archive update task (oeverides base)
		/// </summary>
		/// <returns>TRUE for success, FALSE for failure</returns>
		public override bool PerformTask()
		{

			// Initially set this to true; it will be auto-disabled if an exception occurs while generating the hash
			bool compareWithHash = true;
			int compareErrorCount;
			bool copySuccess;
			bool stageSuccess;

			m_DatasetName = m_TaskParams.GetParam("dataset");

			// Perform base class operations
			if (!base.PerformTask()) return false;

			m_Msg = "Updating dataset " + m_DatasetName + ", job " + m_TaskParams.GetParam("Job");
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_Msg);

			m_ArchiveSharePath = Path.Combine(m_TaskParams.GetParam("Archive_Network_Share_Path"),
														m_TaskParams.GetParam("Folder"));		// Path to archived dataset for Samba operations

			// Verify dataset directory exists in archive
			if (!Directory.Exists(m_ArchiveSharePath))
			{
				m_Msg = "Archived dataset folder " + m_ArchiveSharePath + " not found";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Msg);
				LogOperationFailed(m_DatasetName);
				return false;
			}

			// Set path to results folder on storage server
			m_ResultsFolderPathServer = Path.Combine(m_DSNamePath, m_TaskParams.GetParam("OutputFolderName"));

			// Set the path to the results folder in archive
			m_ResultsFolderPathArchive = Path.Combine(m_ArchiveSharePath, m_TaskParams.GetParam("OutputFolderName"));

			// Initialize the MD5 stage file creator
			InitializeMD5StageFileCreator();


			// Determine if the results folder already exists. If not present, copy entire folder and we're done
			if (!Directory.Exists(m_ResultsFolderPathArchive))
			{
				m_Msg = "Folder " + m_ResultsFolderPathArchive + " not found. Copying from storage server";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_Msg);
				string tmpStr = ConvertSambaPathToLinuxPath(m_ResultsFolderPathArchive);	// Convert to Linux path

				copySuccess = CopyOneFolderToArchive(m_ResultsFolderPathServer, tmpStr);
				if (!copySuccess)
				{
					// If the folder was created in the archive then do not exit this function yet; we need to call CreateMD5StagingFile
					if (!Directory.Exists(m_ResultsFolderPathArchive))
						return false;
				}

				// Create a new stagemd5 file for each file m_ResultsFolderPathServer
				stageSuccess = CreateMD5StagingFile(m_ResultsFolderPathServer, m_ResultsFolderPathArchive);
				if (!stageSuccess)
					return false;

				if (!copySuccess)
					return false;

				// Finished with this update task
				m_Msg = "Completed archive update, dataset " + m_DatasetName + ", Folder " +
								m_TaskParams.GetParam("Output_Folder_Name") + ", job " + m_TaskParams.GetParam("Job");
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_Msg);
				return true;

			}
			else
			{
				List<clsJobData> filesToUpdate = CompareFolders(m_ResultsFolderPathServer, m_ResultsFolderPathArchive, out compareErrorCount, ref compareWithHash);

				// Check for errors
				if (filesToUpdate == null)
				{
					LogOperationFailed(m_DatasetName);
					return false;
				}

				// Check to see if any files needing update were found
				if (filesToUpdate.Count < 1)
				{
					// No files requiring update were found. Human intervention may be required
					m_Msg = "No files needing update found for dataset " + m_DatasetName
									+ ", job " + m_TaskParams.GetParam("Job");
					if (compareErrorCount == 0)
					{
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, m_Msg);
						return true;
					}
					else
					{
						m_Msg += "; however, errors occurred when looking for changed files";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, m_Msg);
						return false;
					}
				}

				// Copy the files needing updating to the archive
				try
				{
					// Open the FTP client
					m_Msg = filesToUpdate.Count.ToString() + " files needing update found. Copying to archive now";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_Msg);
					m_FtpTools = new clsFtpOperations(m_TaskParams.GetParam("Archive_Server"), m_User, m_Pwd,
																	m_UseTls, m_ServerPort);
					m_FtpTools.FtpPassive = m_FtpPassive;
					m_FtpTools.FtpRestart = m_FtpRestart;
					m_FtpTools.FtpTimeOut = m_FtpTimeOut;
					m_FtpTools.UseLogFile = bool.Parse(m_MgrParams.GetParam("ftplogging"));

					if (!m_FtpTools.OpenFTPConnection())
					{
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_FtpTools.ErrMsg);
						LogOperationFailed(m_DatasetName);
						m_FtpTools.CloseFTPConnection();
						if (!string.IsNullOrWhiteSpace(m_FtpTools.ErrMsg))
						{
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_FtpTools.ErrMsg);
						}
						return false;
					}

					copySuccess = UpdateArchive(filesToUpdate, m_ResultsFolderPathArchive, true, m_FtpTools);

					if (!copySuccess)
					{
						m_Msg = "Error updating archive, dataset " + m_DatasetName + ", Job " + m_TaskParams.GetParam("Job");
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Msg);
						LogOperationFailed(m_DatasetName);
						// Do not exit this function yet; we need to call CreateMD5StagingFile
					}

					// Close the FTP server
					m_FtpTools.CloseFTPConnection();
					if (string.IsNullOrWhiteSpace(m_FtpTools.ErrMsg))
					{
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_FtpTools.ErrMsg);
					}

				}
				catch (Exception ex)
				{
					m_Msg = "clsArchiveUpdate.PerformTask, exception performing task";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Msg, ex);
					LogOperationFailed(m_DatasetName);
					return false;
				}

				// Create a new stagemd5 file for each file actually copied
				stageSuccess = CreateMD5StagingFile(m_ResultsFolderPathServer, m_ResultsFolderPathArchive, filesToUpdate);
				if (!stageSuccess)
					return false;

				if (!copySuccess)
				{
					return false;
				}
				else
				{

					if (compareErrorCount == 0)
					{
						// If we got to here, everything worked OK
						m_Msg = "Update complete, dataset " + m_DatasetName;
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_Msg);
						return true;
					}
					else
					{
						// Files have been updated, but errors occurred in CompareFolders
						m_Msg = "Update complete, dataset " + m_DatasetName;
						m_Msg += "; however, errors occurred when looking for changed files";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, m_Msg);
						return false;
					}
				}
			}


		}	// End sub

		/// <summary>
		/// Performs details of archive update operation, using FTP
		/// </summary>
		/// <param name="filesToUpdate">List of clsJobData objects containing files needing updating</param>
		/// <param name="dsArchPath">FTP path to archive</param>
		/// <param name="verifyUpdate">TRUE if verification of successful copy is required; otherwise FALSE</param>
		/// <param name="ftpTools">A clsFtpOperations object</param>
		/// <returns>TRUE for success; FALSE for failure</returns>
		private bool UpdateArchive(List<clsJobData> filesToUpdate, string dsArchPath, bool verifyUpdate, clsFtpOperations ftpTools)
		{
			//Copies a list of files to the archive, renaming old files when necessary
			string archFileName = string.Empty;
			string archPathLinux = string.Empty;

			//Print list of files being updated
			foreach (clsJobData myFileData in filesToUpdate)
			{
				m_Msg = "File: " + myFileData.FileName + ", RenameFlag: " + myFileData.RenameFlag.ToString();
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_Msg);
			}

			foreach (clsJobData FileToUpdate in filesToUpdate)
			{
				archPathLinux = ConvertSambaPathToLinuxPath(dsArchPath);
				archFileName = clsFileTools.CheckTerminator(archPathLinux, false, "/") + FileToUpdate.RelativeFilePath.Replace("\\", "/");
				//Rename file if necessary
				if (FileToUpdate.RenameFlag)
				{
					//A file was already existing in the archive, so rename it with "x_" prefix
					m_Msg = "Renaming file in archive: " + archFileName;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_Msg);
					if (!ftpTools.RenameFile(archFileName, archFileName.Replace(FileToUpdate.FileName, "x_" + FileToUpdate.FileName)))
					{
						//There was a file rename error
						m_Msg = "Error renaming file " + archFileName + "; " + ftpTools.ErrMsg;
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_Msg);
						return false;
					}
				}
				//Copy the file
				m_Msg = "Updating file " + FileToUpdate.SvrFileToUpdate;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_Msg);
				if (!ftpTools.PutFile(FileToUpdate.SvrFileToUpdate, archFileName, verifyUpdate))
				{
					//Error copying the file
					m_Msg = "Error copying file " + FileToUpdate.SvrFileToUpdate + "; " + ftpTools.ErrMsg;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Msg);
					return false;
				}

				FileToUpdate.CopySuccess = true;
			}

			//If we got to here, then everything worked
			return true;

		}	// End sub

		/// <summary>
		/// Converts the Samba version of an archive path to a Linux version of the path
		/// </summary>
		/// <param name="sambaPath">Samba path to convert</param>
		/// <returns>Linux version of input path</returns>
		private string ConvertSambaPathToLinuxPath(string sambaPath)
		{
			string tmpStr;

			// Find index of string "dmsarch" in Samba path
			int startIndx = sambaPath.IndexOf("dmsarch");
			if (startIndx < 0)
			{
				//TODO: Substring wasn't found - this is an error that has to be handled.
				return string.Empty;
			}
			tmpStr = sambaPath.Substring(startIndx);

			// Add on the prefix for the archive
			tmpStr = @"/archive/" + tmpStr;

			// Replace and DOS path separators with Linux separators
			tmpStr = tmpStr.Replace(@"\", @"/");

			return tmpStr;
		}	// End sub

		/// <summary>
		/// Compares folders on storage server and archive
		/// </summary>
		/// <param name="svrFolderPath">Location of source folder on storage server</param>
		/// <param name="sambaFolderPath">Samba path to compared folder in archive</param>
		/// <returns>List of files that need to be copied to the archive</returns>
		private List<clsJobData> CompareFolders(string svrFolderPath, string sambaFolderPath, out int compareErrorCount, ref bool compareWithHash)
		{
			ArrayList serverFiles = null;
			string archFileName;
			int compareResult;
			compareErrorCount = 0;
			string msg;
			clsJobData tmpJobData;

			// Verify server folder exists
			if (!Directory.Exists(svrFolderPath))
			{
				msg = "clsArchiveUpdate.CompareFolders: Storage server folder " + svrFolderPath + " not found";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
				return null;
			}

			// Verify samba folder exists
			if (!Directory.Exists(sambaFolderPath))
			{
				msg = "clsArchiveUpdate.CompareFolders: Archive folder " + sambaFolderPath + " not found";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
				return null;
			}

			// Get a list of all the folders in the server folder
			try
			{
				string[] dirsToScan = { svrFolderPath };
				DirectoryScanner dirScanner = new DirectoryScanner(dirsToScan);
				dirScanner.PerformScan(ref serverFiles, "*");
			}
			catch (Exception ex)
			{
				msg = "clsArchiveUpdate.CompareFolders: Exception getting file listing, folder " + svrFolderPath;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, ex);
				return null;
			}

			// Loop through results folder file list, checking for archive copies and comparing if archive copy present
			List<clsJobData> returnObject = new List<clsJobData>();
			foreach (string svrFileName in serverFiles)
			{
				// Convert the file name on the server to its equivalent in the archive
				archFileName = ConvertServerPathToArchivePath(svrFolderPath, sambaFolderPath, svrFileName);
				if (archFileName.Length == 0)
				{
					msg = "File name not returned when converting from server path to archive path for file" + svrFileName;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
					return null;
				}
				else if (archFileName == "Error")
				{
					msg = "Exception converting server path to archive path for file " + svrFileName + ": " + m_ErrMsg;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
					return null;
				}

				// Determine if file exists in archive
				if (File.Exists(archFileName))
				{
					// File exists in archive, so compare the server and archive versions
					compareResult = CompareTwoFiles(svrFileName, archFileName, compareWithHash);

					if (compareWithHash &&
						compareResult == FILE_COMPARE_ERROR &&
						m_ErrMsg.ToLower().Contains("Exception generating hash".ToLower()))
					{

						// The file most likely could not be retrieved by the tape robot
						// Disable hash-based comparisons for this job

						msg = "clsArchiveUpdate.CompareFolders: Error comparing files. Error msg = " + m_ErrMsg;
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);

						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Disabling hash-based comparisons for this job");

						// Retry the comparison, but this time don't generate a hash
						compareWithHash = false;
						compareResult = CompareTwoFiles(svrFileName, archFileName, compareWithHash);
					}

					switch (compareResult)
					{
						case FILE_COMPARE_EQUAL:
							// Do nothing
							break;
						case FILE_COMPARE_NOT_EQUAL:
							// Add the server file to the list of files to be copied
							tmpJobData = new clsJobData();
							tmpJobData.SvrFileToUpdate = svrFileName;
							tmpJobData.SambaFileToUpdate = archFileName;
							tmpJobData.SvrDsNamePath = svrFolderPath;
							tmpJobData.RenameFlag = true;
							returnObject.Add(tmpJobData);
							break;
						default:        // Includes FILE_COMPARE_ERROR
							// There was a problem with the file comparison; abort the update
							msg = "clsArchiveUpdate.CompareFolders: Error comparing files. Error msg = " + m_ErrMsg;
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
							compareErrorCount += 1;
							break;
					}	// End switch
				}
				else
				{
					// File doesn't exist in archive, so add it to the list of files to be copied
					tmpJobData = new clsJobData();
					tmpJobData.SvrFileToUpdate = svrFileName;
					tmpJobData.SambaFileToUpdate = archFileName;
					tmpJobData.SvrDsNamePath = svrFolderPath;
					tmpJobData.RenameFlag = false;
					returnObject.Add(tmpJobData);
				}
			}	// End foreach

			// All finished, so return
			return returnObject;
		}	// End sub

		/// <summary>
		/// Converts a file path on the storage server to its Samba equivalent
		/// </summary>
		/// <param name="svrPath">Path on server to folder being compared</param>
		/// <param name="archPath">Path in archive to folder being compared</param>
		/// <param name="inpFileName">File being compared</param>
		/// <returns>Full path in archive to file</returns>
		string ConvertServerPathToArchivePath(string svrPath, string archPath, string inpFileName)
		{
			// Convert by replacing storage server path with archive path (Samba version)
			try
			{
				string tmpPath = inpFileName.Replace(svrPath, archPath);
				return tmpPath;
			}
			catch (Exception ex)
			{
				m_ErrMsg = "clsArchiveUpdate.ConvertServerPathToArchivePath: Exception converting path name " + svrPath +
									". Exception message: " + ex.Message;
				return "Error";
			}
		}	// End sub

		/// <summary>
		/// Compares two files, optionally using a SHA hash
		/// </summary>
		/// <param name="InpFile1">Fully qualified path to first file (should reside on the Proto storage server)</param>
		/// <param name="InpFile2">Fully qualified path to second file (should reside in the EMSL archive)</param>
		/// <returns>Integer representing files equal, not equal, or error</returns>
		private int CompareTwoFiles(string srcFileName, string archFileName, bool generateHash)
		{
			m_ErrMsg = string.Empty;

			// First compare the file lengths
			FileInfo fiSourceFile = new FileInfo(srcFileName);
			FileInfo fiArchiveFile = new FileInfo(archFileName);

			if (!fiSourceFile.Exists)
			{
				m_ErrMsg = "clsArchiveUpdate.CompareTwoFiles: File " + fiSourceFile.FullName + " not found ";
				return FILE_COMPARE_ERROR;
			}

			if (!fiArchiveFile.Exists)
			{
				return FILE_COMPARE_NOT_EQUAL;
			}

			if (fiSourceFile.Length != fiArchiveFile.Length)
				return FILE_COMPARE_NOT_EQUAL;

			// Only generate a hash for the files if the archive file was created within the last 35 days
			// Files older than that may be purged from spinning disk and would thus only reside on tape
			// Since retrieval from tape can be slow, we won't compute a hash if the file is more than 35 days old
			if (generateHash && System.DateTime.UtcNow.Subtract(fiArchiveFile.LastWriteTimeUtc).TotalDays < 35)
			{
				// Compares two files via SHA hash
				string sSourceFileHash = string.Empty;
				string sArchiveFileHash = string.Empty;

				// Compute the has for each file
				sSourceFileHash = GenerateHashFromFile(fiSourceFile);
				if (string.IsNullOrEmpty(sSourceFileHash))
				{
					//There was a problem. Description is already in m_ErrMsg
					return FILE_COMPARE_ERROR;
				}

				sArchiveFileHash = GenerateHashFromFile(fiArchiveFile);
				if (string.IsNullOrEmpty(sArchiveFileHash))
				{
					// There was a problem. Description is already in m_ErrMsg
					return FILE_COMPARE_ERROR;
				}

				if (sSourceFileHash == sArchiveFileHash)
				{
					return FILE_COMPARE_EQUAL;
				}
				else
				{
					return FILE_COMPARE_NOT_EQUAL;

				}
			}
			else
			{
				// Simply compare file dates
				// If the source file is newer; then assume we need to copy
				if (fiSourceFile.LastWriteTimeUtc > fiArchiveFile.LastWriteTimeUtc)
					return FILE_COMPARE_NOT_EQUAL;
				else
					return FILE_COMPARE_EQUAL;
			}

		}	// End sub

		/// <summary>
		/// Create a stagemd5 file for all files (and subfolders) in sResultsFolderPathServer
		/// </summary>
		/// <param name="sResultsFolderPathServer"></param>
		/// <param name="sResultsFolderPathArchive"></param>
		/// <returns></returns>
		private bool CreateMD5StagingFile(string sResultsFolderPathServer, string sResultsFolderPathArchive)
		{
			string sLocalParentFolderPathForDataset;
			string sArchiveStoragePathForDataset;

			List<string> lstFilePathsToStage;
			bool bSuccess;

			try
			{
				lstFilePathsToStage = new List<string>();

				// Determine the folder just above sResultsFolderPathServer and just above sResultsFolderPathArchive
				System.IO.DirectoryInfo diResultsFolderServer = new System.IO.DirectoryInfo(sResultsFolderPathServer);
				sLocalParentFolderPathForDataset = diResultsFolderServer.Parent.FullName;

				System.IO.DirectoryInfo diResultsFolderArchive = new System.IO.DirectoryInfo(sResultsFolderPathArchive);
				sArchiveStoragePathForDataset = diResultsFolderArchive.Parent.FullName;

				// Populate lstFilePathsToStage with each file found at sResultsFolderPathServer (including files in subfolders)
				foreach (System.IO.FileInfo fiFile in diResultsFolderServer.GetFiles("*", SearchOption.AllDirectories))
				{
					lstFilePathsToStage.Add(fiFile.FullName);
				}

				bSuccess = CreateMD5StagingFileWork(lstFilePathsToStage, m_DatasetName, sLocalParentFolderPathForDataset, sArchiveStoragePathForDataset);
			}
			catch (Exception ex)
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception creating MD5 staging file for newly archived files from folder " + sResultsFolderPathServer, ex);
				return false;
			}

			return bSuccess;
		}

		/// <summary>
		/// Create a stagemd5 file for all files in filesToUpdate
		/// </summary>
		/// <param name="sResultsFolderPathServer"></param>
		/// <param name="sResultsFolderPathArchive"></param>
		/// <param name="filesToUpdate"></param>
		/// <returns></returns>
		private bool CreateMD5StagingFile(string sResultsFolderPathServer, string sResultsFolderPathArchive, List<clsJobData> filesToUpdate)
		{

			string sLocalParentFolderPathForDataset;
			string sArchiveStoragePathForDataset;

			List<string> lstFilePathsToStage;
			bool bSuccess;

			try
			{
				lstFilePathsToStage = new List<string>();

				// Determine the folder just above sResultsFolderPathServer and just above sResultsFolderPathArchive
				System.IO.DirectoryInfo diResultsFolderServer = new System.IO.DirectoryInfo(sResultsFolderPathServer);
				sLocalParentFolderPathForDataset = diResultsFolderServer.Parent.FullName;

				System.IO.DirectoryInfo diResultsFolderArchive = new System.IO.DirectoryInfo(sResultsFolderPathArchive);
				sArchiveStoragePathForDataset = diResultsFolderArchive.Parent.FullName;

				// Populate lstFilePathsToStage with each file in filesToUpdate
				foreach (clsJobData objFileInfo in filesToUpdate)
				{
					if (objFileInfo.CopySuccess)
						lstFilePathsToStage.Add(objFileInfo.SvrFileToUpdate);
				}

				bSuccess = CreateMD5StagingFileWork(lstFilePathsToStage, m_DatasetName, sLocalParentFolderPathForDataset, sArchiveStoragePathForDataset);
			}
			catch (Exception ex)
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception creating MD5 staging file for newly archived files defined in List filesToUpdate", ex);
				return false;
			}

			return bSuccess;
		}

		private bool CreateMD5StagingFileWork(List<string> lstFilePathsToStage, string sDatasetName, string sLocalParentFolderPathForDataset, string sArchiveStoragePathForDataset)
		{
			const string EXTRA_FILES_REGEX = clsMD5StageFileCreator.EXTRA_FILES_SUFFIX + @"(\d+)$";

			System.Text.RegularExpressions.Regex reExtraFiles;
			System.Text.RegularExpressions.Match reMatch;

			string sDatasetAndSuffix;
			int iExtraFileNumberNew = 1;
			bool bSuccess;

			// Convert sArchiveStoragePathForDataset from the form \\a2.emsl.pnl.gov\dmsarch\LTQ_ORB_2_2\CS_PSK_CC_r4_c_1Aug08_Draco_08-07-14
			// to the form /archive/dmsarch/LTQ_ORB_2_2/CS_PSK_CC_r4_c_1Aug08_Draco_08-07-14

			string sArchiveStoragePathForDatasetUnix = string.Empty;
			bSuccess = clsMD5StageFileCreator.ConvertArchiveSharePathToArchiveStoragePath(sArchiveStoragePathForDataset, false, ref sArchiveStoragePathForDatasetUnix);

			if (!bSuccess)
			{
				string msg = "Error converting the archive folder path (" + sArchiveStoragePathForDataset + ") to the archive storage path (something like /archive/dmsarch/LTQ_ORB_3_1/)";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
				return false;
			}


			// Look for existing stagemd5 or result files for dataset sDatasetName
			System.Collections.Generic.List<string> lstSearchFileSpec = new System.Collections.Generic.List<string>();

			sDatasetAndSuffix = sDatasetName + clsMD5StageFileCreator.EXTRA_FILES_SUFFIX;

			lstSearchFileSpec.Add(clsMD5StageFileCreator.STAGE_FILE_PREFIX + sDatasetAndSuffix + "*");
			lstSearchFileSpec.Add(clsMD5StageFileCreator.STAGE_FILE_INPROGRESS_PREFIX + sDatasetAndSuffix + "*");
			lstSearchFileSpec.Add(clsMD5StageFileCreator.MD5_RESULTS_FILE_PREFIX + sDatasetAndSuffix + "*");
			lstSearchFileSpec.Add(clsMD5StageFileCreator.MD5_RESULTS_INPROGRESS_FILE_PREFIX + sDatasetAndSuffix + "*");

			System.IO.DirectoryInfo diStagingFolder;
			diStagingFolder = new System.IO.DirectoryInfo(mMD5StageFileCreator.StagingFolderPath);

			reExtraFiles = new System.Text.RegularExpressions.Regex(EXTRA_FILES_REGEX, System.Text.RegularExpressions.RegexOptions.Compiled | System.Text.RegularExpressions.RegexOptions.IgnoreCase);

			// Check for each file in lstSearchFileSpec
			foreach (string sFileSpec in lstSearchFileSpec)
			{
				foreach (System.IO.FileInfo fiFile in diStagingFolder.GetFiles(sFileSpec))
				{
					// Examine each file to parse out the number after EXTRA_FILES_SUFFIX
					// For example, if the filename is results.DatasetName__ExtraFiles001 then we want to parse out "001" and convert that to an integer
					reMatch = reExtraFiles.Match(fiFile.Name);
					if (reMatch.Success)
					{
						int iStageFileNum;

						if (int.TryParse(reMatch.Groups[1].Value, out iStageFileNum))
						{
							// Number parsed out
							// Adjust iExtraFileNumberNew if necessary
							if (iStageFileNum >= iExtraFileNumberNew)
								iExtraFileNumberNew = iStageFileNum + 1;
						}
					}
				}
			} // foreach (sFileSpec in lstSearchFileSpec)

			return mMD5StageFileCreator.WriteStagingFile(ref lstFilePathsToStage, sDatasetName, sLocalParentFolderPathForDataset, sArchiveStoragePathForDatasetUnix, iExtraFileNumberNew);

		}


		/// <summary>
		/// Generates SHA1 hash for specified file
		/// </summary>
		/// <param name="InpFileNamePath">Fully qualified path to file</param>
		/// <returns>String representation of SHA1 hash</returns>
		private string GenerateHashFromFile(System.IO.FileInfo fiFile)
		{
			// Generates hash code for specified input file
			byte[] ByteHash = null;

			//Holds hash value returned from hash generator
			SHA1CryptoServiceProvider HashGen = new SHA1CryptoServiceProvider();

			m_ErrMsg = string.Empty;

			FileStream FStream = null;

			try
			{
				//Open the file as a stream for input to the hash class
				FStream = fiFile.OpenRead();
				//Get the file's hash
				ByteHash = HashGen.ComputeHash(FStream);
				return BitConverter.ToString(ByteHash);
			}
			catch (Exception ex)
			{
				m_ErrMsg = "clsArchiveUpdate.GenerateHashFromFile; Exception generating hash for file " + fiFile.FullName + ": " + ex.Message;
				return string.Empty;
			}
			finally
			{
				if ((FStream != null))
				{
					FStream.Close();
				}
			}
		}	// End sub

		#endregion

		#region "MD5StageFileCreator initialization and event handlers"

		private void InitializeMD5StageFileCreator()
		{
			string sArchiveStagingFolderPath;
			sArchiveStagingFolderPath = m_MgrParams.GetParam("HashFileLocation");

			if (string.IsNullOrWhiteSpace(sArchiveStagingFolderPath))
			{
				sArchiveStagingFolderPath = MD5StageFileCreator.clsMD5StageFileCreator.DEFAULT_STAGING_FOLDER_PATH;
				string msg = "Manager parameter HashFileLocation is not defined; will use default path of '" + sArchiveStagingFolderPath + "'";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);
			}

			mMD5StageFileCreator = new MD5StageFileCreator.clsMD5StageFileCreator(sArchiveStagingFolderPath);

			// Attach the events
			mMD5StageFileCreator.OnErrorEvent += new MD5StageFileCreator.clsMD5StageFileCreator.OnErrorEventEventHandler(MD5ErrorEventHandler);
			mMD5StageFileCreator.OnMessageEvent += new MD5StageFileCreator.clsMD5StageFileCreator.OnMessageEventEventHandler(MD5MessageEventHandler);
		}

		private void MD5ErrorEventHandler(string sErrorMessage)
		{
			string msg = "MD5StageFileCreator Error: " + sErrorMessage;
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
		}

		private void MD5MessageEventHandler(string sMessage)
		{
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, sMessage);
		}

		#endregion


	}	// End class
}	// End namespace
