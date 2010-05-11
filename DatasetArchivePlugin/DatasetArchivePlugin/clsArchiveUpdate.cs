
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
			string m_SambaNamePath;
			string m_ResultsFolderPathArchive;
			string m_ResultsFolderPathServer;
		#endregion

		#region "Constructors"
			/// <summary>
			/// Constructor
			/// </summary>
			/// <param name="MgrParams">Manager parameters</param>
			/// <param name="TaskParasms">Task parameters</param>
			public clsArchiveUpdate(IMgrParams MgrParams, ITaskParams TaskParasms)
				: base(MgrParams, TaskParasms)
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
				// ToDo: Add job duration stuff?
		
				// Perform base class operations
				if (!base.PerformTask()) return false;

				m_Msg = "Updating dataset " + m_TaskParams.GetParam("dataset") + ", job " + m_TaskParams.GetParam("Job");
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.INFO, m_Msg);

				m_SambaNamePath = Path.Combine(m_TaskParams.GetParam("Archive_Network_Share_Path"),
															m_TaskParams.GetParam("Folder"));		// Path to archived dataset for Samba operations

				// Verify dataset directory exists in archive
				if (!Directory.Exists(m_SambaNamePath))
				{
					m_Msg = "Archived dataset folder " + m_SambaNamePath + " not found";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile,clsLogTools.LogLevels.ERROR,m_Msg);
					LogOperationFailed(m_TaskParams.GetParam("dataset"));
					return false;
				}

				// Set path to results folder on storage server
				m_ResultsFolderPathServer = Path.Combine(m_DSNamePath, m_TaskParams.GetParam("OutputFolderName"));

				// Set the path to the results folder in archive
				m_ResultsFolderPathArchive = Path.Combine(m_SambaNamePath, m_TaskParams.GetParam("OutputFolderName"));


				// Determine if the results folder already exists. If not present, copy entire folder and we're done
				if (!Directory.Exists(m_ResultsFolderPathArchive))
				{
					m_Msg = "Folder " + m_ResultsFolderPathArchive + " not found. Copying from storage server";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_Msg);
					string tmpStr = ConvertSambaPathToLinuxPath(m_ResultsFolderPathArchive);	// Convert to Linux path
					if (!CopyOneFolderToArchive(m_ResultsFolderPathServer, tmpStr)) return false;

					// Finished with this update task
					m_Msg = "Completed archive update, dataset " + m_TaskParams.GetParam("dataset") + ", Folder " +
									m_TaskParams.GetParam("Output_Folder_Name") + ", job " + m_TaskParams.GetParam("Job");
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.INFO, m_Msg);
					return true;
				}	// End "results folder not in archive" actions
				else
				{
					List<clsJobData> filesToUpdate = CompareFolders(m_ResultsFolderPathServer, m_ResultsFolderPathArchive);
					
					// Check for errors
					if (filesToUpdate == null)
					{
						LogOperationFailed(m_TaskParams.GetParam("dataset"));
						return false;
					}

					// Check to see if any files needing update were found
					if (filesToUpdate.Count < 1)
					{
						// No files requiring update were found. Human intervention may be required
						m_Msg = "No files needing update found for dataset " + m_TaskParams.GetParam("dataset")
										+ ", job " + m_TaskParams.GetParam("Job");
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, m_Msg);
						return true;
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
							LogOperationFailed(m_TaskParams.GetParam("dataset"));
							m_FtpTools.CloseFTPConnection();
							if (m_FtpTools.ErrMsg != "")
							{
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_FtpTools.ErrMsg);
							}
							return false;
						}
						if (!UpdateArchive(filesToUpdate, m_ResultsFolderPathArchive, true, m_FtpTools))
						{
							m_Msg = "Error updating archive, dataset " + m_TaskParams.GetParam("dataset")
										+ ", Job " + m_TaskParams.GetParam("Job");
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Msg);
							LogOperationFailed(m_TaskParams.GetParam("dataset"));
							m_FtpTools.CloseFTPConnection();
							if (m_FtpTools.ErrMsg != "")
							{
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_FtpTools.ErrMsg);
							}
							return false;
						}
						
						// Close the FTP server
						m_FtpTools.CloseFTPConnection();
						if (m_FtpTools.ErrMsg != "")
						{
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_FtpTools.ErrMsg);
						}
					}
					catch (Exception ex)
					{
						m_Msg = "clsArchiveUpdate.PerformTask, exception performing task";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Msg, ex);
						LogOperationFailed(m_TaskParams.GetParam("dataset"));
						return false;
					}
				}

				// If we got to here, everything worked OK
				m_Msg = "Update complete, dataset " + m_TaskParams.GetParam("dataset");
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.INFO, m_Msg);
				return true;
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
				string archFileName = "";
				string archPathLinux = "";

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
					return "";
				}
				tmpStr = sambaPath.Substring(startIndx);
				
				// Add on the prefix for the archive
				tmpStr = @"/nwfs/" + tmpStr;

				// Replace and DOS path separators with Linux separators
				tmpStr = tmpStr.Replace(@"\",@"/");

				return tmpStr;
			}	// End sub

			/// <summary>
			/// Compares folders on storage server and archive
			/// </summary>
			/// <param name="svrFolderPath">Location of source folder on storage server</param>
			/// <param name="sambaFolderPath">Samba path to compared folder in archive</param>
			/// <returns>List of files that need to be copied to the archive</returns>
			private List<clsJobData> CompareFolders(string svrFolderPath, string sambaFolderPath)
			{
				ArrayList serverFiles = null;
				string archFileName;
				int compareResult;
				string msg;
				clsJobData tmpJobData;

				// Verify server folder exists
				if (!Directory.Exists(svrFolderPath))
				{
					msg = "clsArchiveUpdate.CompareFolders: Storage server folder " + svrFolderPath + " not found";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile,clsLogTools.LogLevels.ERROR,msg);
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
						compareResult = CompareTwoFiles(svrFileName, archFileName);
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
							default:
								// There was a problem with the file comparison
								msg = "clsArchiveUpdate.CompareFolders: Error comparing files. Error msg = " + m_ErrMsg;
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
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
			/// Compares two files using SHA hash
			/// </summary>
			/// <param name="InpFile1">Fully qualified path to first file</param>
			/// <param name="InpFile2">Fully qualified path to second file</param>
			/// <returns>Integer representing files equal, not equal, or error</returns>
			private int CompareTwoFiles(string inpFile1, string inpFile2)
			{
				// Compares two files via SHA hash
				string file1Hash = "";	//String version of InpFile1 hash
				string file2Hash = "";	//String version of InpFile2 hash

				m_ErrMsg = "";

				//Get hash's for both files
				file1Hash = GenerateHashFromFile(inpFile1);
				if (string.IsNullOrEmpty(file1Hash))
				{
					//There was a problem. Description is already in m_ErrMsg
					return FILE_COMPARE_ERROR;
				}
				file2Hash = GenerateHashFromFile(inpFile2);
				if (string.IsNullOrEmpty(file2Hash))
				{
					//There was a problem. Description is already in m_ErrMsg
					return FILE_COMPARE_ERROR;
				}

				if (file1Hash == file2Hash)
				{
					return FILE_COMPARE_EQUAL;
				}
				else
				{
					//Files not equal
					return FILE_COMPARE_NOT_EQUAL;

				}
			}	// End sub

			/// <summary>
			/// Generates SHA1 hash for specified file
			/// </summary>
			/// <param name="InpFileNamePath">Fully qualified path to file</param>
			/// <returns>String representation of SHA1 hash</returns>
			private string GenerateHashFromFile(string InpFileNamePath)
			{
				// Generates hash code for specified input file
				byte[] ByteHash = null;

				//Holds hash value returned from hash generator
				SHA1CryptoServiceProvider HashGen = new SHA1CryptoServiceProvider();

				m_ErrMsg = "";

				//Verify input file exists
				if (!File.Exists(InpFileNamePath))
				{
					m_ErrMsg = "clsArchiveUpdate.GenerateHashFromFile: File " + InpFileNamePath + " not found ";
					return "";
				}

				FileInfo Fi = new FileInfo(InpFileNamePath);
				Stream FStream = null;

				try
				{
					//Open the file as a stream for input to the hash class
					FStream = Fi.OpenRead();
					//Get the file's hash
					ByteHash = HashGen.ComputeHash(FStream);
					return BitConverter.ToString(ByteHash);
				}
				catch (Exception ex)
				{
					m_ErrMsg = "clsArchiveUpdate.GenerateHashFromFile; Exception generating hash for file " + InpFileNamePath + ": " + ex.Message;
					return "";
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
	}	// End class
}	// End namespace
