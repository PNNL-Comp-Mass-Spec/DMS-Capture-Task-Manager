
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
using System.Collections.Generic;
using CaptureTaskManager;
using System.IO;
using PRISM.Files;
using System.Security.Cryptography;

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

		string m_ArchiveSharePath = string.Empty;				// The dataset folder path in the archive, for example: \\a2.emsl.pnl.gov\dmsarch\VOrbiETD03\2013_2\QC_Shew_13_02_C_29Apr13_Cougar_13-03-25
		string m_ResultsFolderPathArchive = string.Empty;		// The target path to copy the data to, for example:    \\a2.emsl.pnl.gov\dmsarch\VOrbiETD03\2013_2\QC_Shew_13_02_C_29Apr13_Cougar_13-03-25\SIC201304300029_Auto938684
		string m_ResultsFolderPathServer = string.Empty;		// The source path of the dataset folder (or dataset job results folder) to archive, for example: \\proto-7\VOrbiETD03\2013_2\QC_Shew_13_02_C_29Apr13_Cougar_13-03-25\SIC201304300029_Auto938684

		#endregion

		#region "Auto-Properties"

#if !DartFTPMissing
		[Obsolete("No longer needed since using MyEMSL")]
		public bool CreateDatasetFolderInArchiveIfMissing { get; set; }
#endif

		#endregion

		#region "Constructors"

		/// <summary>
		/// Constructor
		/// </summary>
		/// <param name="MgrParams">Manager parameters</param>
		/// <param name="TaskParams">Task parameters</param>
		/// <param name="StatusTools"></param>
		public clsArchiveUpdate(IMgrParams MgrParams, ITaskParams TaskParams, IStatusFile StatusTools)
			: base(MgrParams, TaskParams, StatusTools)
		{
#if !DartFTPMissing
				CreateDatasetFolderInArchiveIfMissing = false;
#endif
		}
		#endregion

		#region "Methods"
		/// <summary>
		/// Performs an archive update task (overrides base)
		/// </summary>
		/// <returns>TRUE for success, FALSE for failure</returns>
		public override bool PerformTask()
		{

			// Perform base class operations
			if (!base.PerformTask()) return false;

			m_Msg = "Updating dataset " + m_DatasetName + ", job " + m_TaskParams.GetParam("Job");
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_Msg);

			const bool onlyUseMyEMSL = true;

#if !DartFTPMissing
			string instrumentName = m_TaskParams.GetParam("Instrument_Name");
			bool onlyUseMyEMSL = OnlyUseMyEMSL(instrumentName);
#endif

			bool pushDatasetToMyEmsl = m_TaskParams.GetParam("PushDatasetToMyEMSL", false);

			if (pushDatasetToMyEmsl || onlyUseMyEMSL)
			{
				m_Msg = "Pushing dataset folder to MyEMSL";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_Msg);

				mMostRecentLogTime = DateTime.UtcNow;
				mLastStatusUpdateTime = DateTime.UtcNow;

				const int iMaxMyEMSLUploadAttempts = 2;
				bool recurse = m_TaskParams.GetParam("PushDatasetRecurse", false);
				if (onlyUseMyEMSL)
					recurse = true;

				// Set this to true to create the .tar file locally and thus not upload the data to MyEMSL
                var debugMode = Pacifica.Core.EasyHttp.eDebugMode.DebugDisabled;

                if (m_TaskParams.GetParam("DebugTestTar", false))
                    debugMode = Pacifica.Core.EasyHttp.eDebugMode.CreateTarLocal;
                else
                if (m_TaskParams.GetParam("MyEMSLOffline", false))
                    debugMode = Pacifica.Core.EasyHttp.eDebugMode.MyEMSLOfflineMode;

				bool copySuccess = UploadToMyEMSLWithRetry(iMaxMyEMSLUploadAttempts, recurse, debugMode);

				if (!copySuccess)
					return false;

			}

			if (onlyUseMyEMSL)
			{
				// Finished with this update task
				m_Msg = "Completed push to MyEMSL, dataset " + m_DatasetName + ", Folder " +
								m_TaskParams.GetParam("OutputFolderName") + ", job " + m_TaskParams.GetParam("Job");
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_Msg);
				return true;
			}

#if DartFTPMissing
			return true;			
#else
			bool success = PerformTaskUseFTP();
			return success;
#endif
		}

#if !DartFTPMissing
		[Obsolete("No longer needed since using MyEMSL")]
		protected bool PerformTaskUseFTP()
		{
			// Initially set this to true; it will be auto-disabled if an exception occurs while generating the hash
			bool compareWithHash = true;
			int compareErrorCount;
			bool copySuccess;
			bool stageSuccess;
			string ftpErrMsg;

			bool accessDeniedViaSamba = false;

			m_ArchiveSharePath = Path.Combine(m_TaskParams.GetParam("Archive_Network_Share_Path"),
														m_TaskParams.GetParam("Folder"));		// Path to archived dataset for Samba operations

			// Verify dataset directory exists in archive
			if (!Directory.Exists(m_ArchiveSharePath))
			{

				if (CreateDatasetFolderInArchiveIfMissing)
				{
					string msg = "Archived dataset folder not found; will create it at " + m_ArchiveSharePath;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);
				}
				else				
				{
					m_Msg = "Archived dataset folder not found: " + m_ArchiveSharePath;
					LogErrorMessage(m_Msg, "Verify dataset directory exists in archive");					
				}

				// Dataset folder not found; look for the parent folder
				System.IO.DirectoryInfo diParentFolder = new System.IO.DirectoryInfo(m_ArchiveSharePath).Parent;
				if (diParentFolder.Exists)
				{
					// Parent folder exists; can we enumerate its files and folders?
					try
					{
						System.IO.FileSystemInfo[] fiChildren = diParentFolder.GetFileSystemInfos();

					}
					catch (Exception ex)
					{
						if (ex.Message.Contains("Access") && ex.Message.Contains("is denied"))
							accessDeniedViaSamba = true;
					}

					if (accessDeniedViaSamba)
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Parent folder exists, but directory security issues are not allowing us to view the subdirectories; will assume all files should be updated via FTP");

				}

				if (!accessDeniedViaSamba && !CreateDatasetFolderInArchiveIfMissing)
				{					
					LogOperationFailed(m_DatasetName);
					return false;
				}
			}

			// Set path to results folder on storage server (OutputFolderName might be blank)
			m_ResultsFolderPathServer = Path.Combine(m_DSNamePath, m_TaskParams.GetParam("OutputFolderName", String.Empty));

			// Set the path to the results folder in archive
			m_ResultsFolderPathArchive = Path.Combine(m_ArchiveSharePath, m_TaskParams.GetParam("OutputFolderName", String.Empty));

			// Determine if the results folder already exists. If not present, copy entire folder and we're done
			if (accessDeniedViaSamba || !Directory.Exists(m_ResultsFolderPathArchive))
			{
				m_Msg = "Folder " + m_ResultsFolderPathArchive + " not found. Copying from storage server";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_Msg);
				string resultsFolderPathLinux = ConvertSambaPathToLinuxPath(m_ResultsFolderPathArchive);	// Convert to Linux path

				copySuccess = CopyOneFolderToArchive(m_ResultsFolderPathServer, resultsFolderPathLinux);
				if (!copySuccess)
				{
					// If the folder was created in the archive then do not exit this function yet; we need to call CreateMD5StagingFile
					if (!Directory.Exists(m_ResultsFolderPathArchive))
						return false;
				}

				// Create a new stagemd5 file for each file m_ResultsFolderPathServer
				stageSuccess = CreateMD5StagingFile(m_ResultsFolderPathServer, m_ResultsFolderPathArchive);
				if (!stageSuccess)
				{
					LogErrorMessage("CreateMD5StagingFile returned false for " + m_DatasetName, "CreateMD5StagingFile");

					// Temporarily ignoring, effective January 31, 2013 at 10:45 pm
					// return false;
					m_WarningMsg = "CreateMD5StagingFile returned false";
				}					

				if (!copySuccess)
					return false;

				// Finished with this update task
				m_Msg = "Completed archive update, dataset " + m_DatasetName + ", Folder " +
								m_TaskParams.GetParam("OutputFolderName") + ", job " + m_TaskParams.GetParam("Job");
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_Msg);
				return true;

			}
			else
			{
				if (accessDeniedViaSamba)
					m_Msg = "Unable to determine if " + m_ResultsFolderPathArchive + " exists since accessDeniedViaSamba = True";
				else
					m_Msg = "Folder " + m_ResultsFolderPathArchive + " exists; comparing remote files to local files";

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, m_Msg);

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
					m_Msg = "No files needing update found for dataset " + m_DatasetName + ", job " + m_TaskParams.GetParam("Job");
					
					if (string.IsNullOrWhiteSpace(m_Msg))
						m_Msg = "No files needing update found";

					if (compareErrorCount == 0)
					{
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, m_Msg);
						return true;
					}
					else
					{
						m_Msg += "; however, errors occurred when looking for changed files";
						LogErrorMessage(m_Msg, "No files requiring update were found. Human intervention may be required", true);
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
						LogErrorMessage(m_FtpTools.ErrMsg, "Open the FTP client");						
						LogOperationFailed(m_DatasetName);
						m_FtpTools.CloseFTPConnection();
						ftpErrMsg = m_FtpTools.ErrMsg;
						if (!string.IsNullOrWhiteSpace(ftpErrMsg))
						{
							LogErrorMessage(ftpErrMsg, "Close the FTP client");
						}
						return false;
					}

					copySuccess = UpdateArchive(filesToUpdate, m_ResultsFolderPathArchive, true, m_FtpTools);

					if (!copySuccess)
					{
						m_Msg = "Error updating archive, dataset " + m_DatasetName + ", Job " + m_TaskParams.GetParam("Job");
						LogErrorMessage(m_Msg, "Error updating archive");
						LogOperationFailed(m_DatasetName);
						// Do not exit this function yet; we need to call CreateMD5StagingFile
					}

					// Close the FTP server
					m_FtpTools.CloseFTPConnection();
					ftpErrMsg = m_FtpTools.ErrMsg;
					if (!string.IsNullOrWhiteSpace(ftpErrMsg))
					{
						LogErrorMessage(ftpErrMsg, "Close the FTP client");
					}

					string msg = "Updated " + filesToUpdate.Count + " file";
					if (filesToUpdate.Count != 1)
						msg += "s";
					msg += " via FTP";

					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
					
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
				{					
					LogErrorMessage("CreateMD5StagingFile returned false for " + m_DatasetName, "CreateMD5StagingFile");

					// Temporarily ignoring, effective January 31, 2013 at 10:45 pm
					// return false;
					m_WarningMsg = "CreateMD5StagingFile returned false";
				}

				if (!copySuccess)				
					return false;				
				
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
					LogErrorMessage(m_Msg, "Files have been updated, but errors occurred in CompareFolders", true);
					return false;
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
		[Obsolete("No longer needed since using MyEMSL")]
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

			archPathLinux = ConvertSambaPathToLinuxPath(dsArchPath);
			archPathLinux = clsFileTools.CheckTerminator(archPathLinux, false, "/");

			foreach (clsJobData FileToUpdate in filesToUpdate)
			{
				archFileName = FileToUpdate.RelativeFilePath.Replace(@"\", "/");
				if (!archFileName.StartsWith("/"))
					archFileName = "/" + archFileName;
				archFileName = archPathLinux + archFileName;

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
					LogErrorMessage(m_Msg, "Error copying file");
					return false;
				}

				FileToUpdate.CopySuccess = true;
			}

			//If we got to here, then everything worked
			return true;

		}	// End sub
#endif

		/// <summary>
		/// Converts the Samba version of an archive path to a Linux version of the path
		/// </summary>
		/// <param name="sambaPath">Samba path to convert</param>
		/// <returns>Linux version of input path</returns>
		private string ConvertSambaPathToLinuxPath(string sambaPath)
		{
			// Find index of string "dmsarch" in Samba path
			int startIndx = sambaPath.IndexOf("dmsarch");
			if (startIndx < 0)
			{
				//TODO: Substring wasn't found - this is an error that has to be handled.
				return string.Empty;
			}
			string tmpStr = sambaPath.Substring(startIndx);

			// Add on the prefix for the archive
			tmpStr = "/archive/" + tmpStr;

			// Replace and DOS path separators with Linux separators
			tmpStr = tmpStr.Replace(@"\", "/");

			return tmpStr;
		}	// End sub

		/// <summary>
		/// Compares folders on storage server and archive
		/// </summary>
		/// <param name="svrFolderPath">Location of source folder on storage server</param>
		/// <param name="sambaFolderPath">Samba path to compared folder in archive</param>
		/// <param name="compareErrorCount"></param>
		/// <param name="compareWithHash"></param>
		/// <returns>List of files that need to be copied to the archive</returns>
		private List<clsJobData> CompareFolders(string svrFolderPath, string sambaFolderPath, out int compareErrorCount, ref bool compareWithHash)
		{
			List<string> serverFiles;
			compareErrorCount = 0;
			string msg;

			// Verify server folder exists
			if (!Directory.Exists(svrFolderPath))
			{
				msg = "clsArchiveUpdate.CompareFolders: Storage server folder " + svrFolderPath + " not found";
				LogErrorMessage(msg, "Current Task");
				return null;
			}

			// Verify samba folder exists
			if (!Directory.Exists(sambaFolderPath))
			{
				msg = "clsArchiveUpdate.CompareFolders: Archive folder " + sambaFolderPath + " not found";
				LogErrorMessage(msg, "Current Task");
				return null;
			}

			// Get a list of all the folders in the server folder
			try
			{
				var dirsToScan = new List<string> { svrFolderPath };
				var dirScanner = new DirectoryScanner(dirsToScan);
				serverFiles = dirScanner.PerformScan("*");
			}
			catch (Exception ex)
			{
				msg = "clsArchiveUpdate.CompareFolders: Exception getting file listing, folder " + svrFolderPath;
				LogErrorMessage(msg + "; " + ex.Message, "Exception getting file listing for svrFolderPath");
				return null;
			}

			// Loop through results folder file list, checking for archive copies and comparing if archive copy present
			var returnObject = new List<clsJobData>();
			foreach (string svrFileName in serverFiles)
			{
				// Convert the file name on the server to its equivalent in the archive
				string archFileName = ConvertServerPathToArchivePath(svrFolderPath, sambaFolderPath, svrFileName);
				if (archFileName.Length == 0)
				{
					msg = "File name not returned when converting from server path to archive path for file" + svrFileName;
					LogErrorMessage(msg, "Current Task");
					return null;
				}
				
				if (archFileName == "Error")
				{
					msg = "Exception converting server path to archive path for file " + svrFileName + ": " + m_ErrMsg;
					LogErrorMessage(msg, "Current Task");
					return null;
				}

				// Determine if file exists in archive
				clsJobData tmpJobData;
				if (File.Exists(archFileName))
				{
					// File exists in archive, so compare the server and archive versions
					int compareResult = CompareTwoFiles(svrFileName, archFileName, compareWithHash);

					if (compareWithHash &&
						compareResult == FILE_COMPARE_ERROR &&
						m_ErrMsg.ToLower().Contains("Exception generating hash".ToLower()))
					{

						// The file most likely could not be retrieved by the tape robot
						// Disable hash-based comparisons for this job

						msg = "clsArchiveUpdate.CompareFolders: Error comparing files. Error msg = " + m_ErrMsg;
						LogErrorMessage(msg, "Current Task");

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
							tmpJobData = new clsJobData
							{
								SvrFileToUpdate = svrFileName,
								SambaFileToUpdate = archFileName,
								SvrDsNamePath = svrFolderPath,
								RenameFlag = true
							};
							returnObject.Add(tmpJobData);
							break;
						default:        // Includes FILE_COMPARE_ERROR
							// There was a problem with the file comparison; abort the update
							msg = "clsArchiveUpdate.CompareFolders: Error comparing files. Error msg = " + m_ErrMsg;
							LogErrorMessage(msg, "Current Task");
							compareErrorCount += 1;
							break;
					}	// End switch
				}
				else
				{
					// File doesn't exist in archive, so add it to the list of files to be copied
					tmpJobData = new clsJobData
					{
						SvrFileToUpdate = svrFileName,
						SambaFileToUpdate = archFileName,
						SvrDsNamePath = svrFolderPath,
						RenameFlag = false
					};
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
		/// <param name="srcFileName">Fully qualified path to first file (should reside on the Proto storage server)</param>
		/// <param name="archFileName">Fully qualified path to second file (should reside in the EMSL archive)</param>
		/// <param name="generateHash"></param>
		/// <returns>Integer representing files equal, not equal, or error</returns>
		private int CompareTwoFiles(string srcFileName, string archFileName, bool generateHash)
		{
			m_ErrMsg = string.Empty;

			// First compare the file lengths
			var fiSourceFile = new FileInfo(srcFileName);
			var fiArchiveFile = new FileInfo(archFileName);

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
			if (generateHash && DateTime.UtcNow.Subtract(fiArchiveFile.LastWriteTimeUtc).TotalDays < 35)
			{
				// Compares two files via SHA hash

				// Compute the has for each file
				string sSourceFileHash = GenerateHashFromFile(fiSourceFile);
				if (string.IsNullOrEmpty(sSourceFileHash))
				{
					//There was a problem. Description is already in m_ErrMsg
					return FILE_COMPARE_ERROR;
				}

				string sArchiveFileHash = GenerateHashFromFile(fiArchiveFile);
				if (string.IsNullOrEmpty(sArchiveFileHash))
				{
					// There was a problem. Description is already in m_ErrMsg
					return FILE_COMPARE_ERROR;
				}

				if (sSourceFileHash == sArchiveFileHash)
					return FILE_COMPARE_EQUAL;
				else	
					return FILE_COMPARE_NOT_EQUAL;
			}
			
			// Simply compare file dates
			// If the source file is newer; then assume we need to copy
			if (fiSourceFile.LastWriteTimeUtc > fiArchiveFile.LastWriteTimeUtc)
				return FILE_COMPARE_NOT_EQUAL;
			else
				return FILE_COMPARE_EQUAL;

		}	// End sub

		/// <summary>
		/// Generates SHA1 hash for specified file
		/// </summary>
		/// <param name="fiFile">Fileinfo object</param>
		/// <returns>String representation of SHA1 hash</returns>
		private string GenerateHashFromFile(FileInfo fiFile)
		{
			// Generates hash code for specified input file

			//Holds hash value returned from hash generator
			var HashGen = new SHA1CryptoServiceProvider();

			m_ErrMsg = string.Empty;

			FileStream FStream = null;

			try
			{
				//Open the file as a stream for input to the hash class
				FStream = fiFile.OpenRead();
				//Get the file's hash
				byte[] ByteHash = HashGen.ComputeHash(FStream);
				return BitConverter.ToString(ByteHash).Replace("-", string.Empty).ToLower();
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

		/// <summary>
		/// Write an error message to the log
		/// If msg is blank, then logs the current task description followed by "empty error message"
		/// </summary>
		/// <param name="msg">Error message</param>
		/// <param name="currentTask">Current task</param>
		protected void LogErrorMessage(string msg, string currentTask)
		{
			LogErrorMessage(msg, currentTask, false);
		}

		/// <summary>
		/// Write an error message to the log
		/// If msg is blank, then logs the current task description followed by "empty error message"
		/// </summary>
		/// <param name="msg">Error message</param>
		/// <param name="currentTask">Current task</param>
		/// <param name="logDB">True to log to the database in addition to logging to the local log file</param>
		protected void LogErrorMessage(string msg, string currentTask, bool logDB)
		{
			if (string.IsNullOrWhiteSpace(msg))
				msg = currentTask + ": empty error message";

			if (logDB)
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
			else
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
		}

		#endregion


	}	// End class
}	// End namespace
