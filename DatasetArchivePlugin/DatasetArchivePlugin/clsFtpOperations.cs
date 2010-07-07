
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 10/08/2009
//
// Last modified 10/08/2009
//*********************************************************************************************************
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Dart.PowerTCP.Ftp;
using PRISM.Files;
using CaptureTaskManager;
using System.CodeDom;
using System.Windows.Forms;
using System.Collections.Specialized;

namespace DatasetArchivePlugin
{
	class clsFtpOperations
	{
		//*********************************************************************************************************
		// Class for handling all operations involving the FTP client
		//**********************************************************************************************************

		#region "Enums"
			//Enum for FolderExists and FileExists returns
			public enum EnumFileFolderExists : short
			{
				Found = 0,
				NotFound = 1,
				FindError = 2
			}
		#endregion

		#region "Class variables"
			private Dart.PowerTCP.Ftp.Ftp m_FtpDart;
			private string m_User;
			private string m_Pwd;
			private string m_ErrMsg;
			private string m_Server;
			private bool m_LogFile = false;
			private bool m_UseTLS = false;
			private int m_ServerPort = 21;
			private int m_FtpTimeout = 30000;	// 30 seconds
			private bool m_FtpPassive = true;	// use "PASV" on connection instead of "PORT"
			private bool m_FtpRestart = false;	// file has to be received in single transfer
		#endregion

		#region "Properties"
			public bool UseLogFile
			{
				get { return m_LogFile; }
				set { m_LogFile = value; }
			}

			public bool Connected
			{
				get { return m_FtpDart.Connected; }
			}

			public int ServerPort
			{
				get { return m_ServerPort; }
				set { m_ServerPort = value; }
			}

			public bool UseTLS
			{
				get { return m_UseTLS; }
				set { m_UseTLS = value; }
			}

			public int FtpTimeOut
			{
				get { return m_FtpTimeout; }
				set { m_FtpTimeout = value; }
			}

			public bool FtpPassive
			{
				get { return m_FtpPassive; }
				set { m_FtpPassive = value; }
			}

			public bool FtpRestart
			{
				get { return m_FtpRestart; }
				set { m_FtpRestart = value; }
			}

			public string ErrMsg
			{
				get { return m_ErrMsg; }
			}
			
		#endregion

		#region "Constructors"
			/// <summary>
			/// Constructor (overload assumes no security settings)
			/// </summary>
			/// <param name="Server">FTP server name</param>
			/// <param name="User">FTP account name</param>
			/// <param name="Pwd">Account password</param>
			public clsFtpOperations(string Server, string User, string Pwd)
			{
   			 m_Server = Server;
   			 m_User = User;
   			 m_Pwd = Pwd;
			}	// End sub

			/// <summary>
			/// Constructor (overload allows use of security settings)
			/// </summary>
			/// <param name="Server">FTP server name</param>
			/// <param name="User">FTP account name</param>
			/// <param name="Pwd">Account password</param>
			/// <param name="UseTLS">TRUE to use TLS security settings</param>
			/// <param name="ServerPort">Port for TLS connections</param>
			public clsFtpOperations(string Server, string User, string Pwd, bool UseTLS, int ServerPort)
			{
   			 m_Server = Server;
   			 m_User = User;
   			 m_Pwd = Pwd;
   			 m_UseTLS = UseTLS;
   			 m_ServerPort = ServerPort;
			}	// End sub
		#endregion

		#region "Methods"
			/// <summary>
			/// Opens an FTP connection
			/// </summary>
			/// <returns>TRUE for success, FALSE for failure</returns>
			/// <remarks>Assumes password property has been set and password has been encrypted using pwd.exe routine</remarks>
			public bool OpenFTPConnection()
			{
				string TempPassword = "";
				string msg;

				msg = "Opening FTP connection";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,msg);

				m_FtpDart = new Dart.PowerTCP.Ftp.Ftp();
				m_FtpDart.Trace += new SegmentEventHandler(On_FPT_Dart_Trace);	// Subscribe event handler for Trace event
				m_FtpDart.Username = m_User;
				TempPassword = m_Pwd;
				m_FtpDart.Password = DecodePassword(TempPassword);	//Convert encrypted password to real password
				m_FtpDart.Server = m_Server;
				m_FtpDart.FileType = FileType.Image;	// binary mode
				m_FtpDart.DoEvents = false;
				m_FtpDart.Passive = m_FtpPassive;
				m_FtpDart.Restart = m_FtpRestart;
				m_FtpDart.Timeout = m_FtpTimeout;
				if (m_UseTLS)
				{
					m_FtpDart.Security = Security.ExplicitControlOnly;
					m_FtpDart.ServerPort = m_ServerPort;
					m_FtpDart.UseAuthentication = false;
				}
				else
				{
					m_FtpDart.Security = Security.None;
				}
				//The DART FTP control doesn't actually open the connection until the first command is sent,
				//	so send a NOP and verify communication is OK
				try
				{
					if (m_UseTLS)
					{
						//These two commands turn off the data channel encryption after logon for secure connections
						m_FtpDart.Invoke(FtpCommand.Null, "PBSZ 0");
						m_FtpDart.Invoke(FtpCommand.Null, "PROT C");
					}
					m_FtpDart.Invoke(FtpCommand.NoOp);
					return true;
				}
				catch (Exception ex)
				{
					m_ErrMsg = "Error opening FTP connection, " + StripPwd(ex.Message);
					return false;
				}
			}	// End sub

			/// <summary>
			/// Closes an FTP connection
			/// </summary>
			public void CloseFTPConnection()
			{
				m_ErrMsg = "";

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile,clsLogTools.LogLevels.DEBUG,"Closing FTP connection");
				try
				{
					m_FtpDart.Trace -= On_FPT_Dart_Trace;	// Unsubscribe Trace event
					m_FtpDart.Close();
					m_FtpDart.Dispose();
				}
				catch (Exception ex)
				{
					m_ErrMsg = "clsFTPOperations.CloseFTPConnection, Exception closing connection: " + ex.Message;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_ErrMsg, ex);
				}
			}	// End sub

			/// <summary>
			/// Copy folder given by sourcePath to be a subfolder under storagePath
			/// </summary>
			/// <param name="SourcePath">Full path of folder to be copied</param>
			/// <param name="StoragePath">Destination folder</param>
			/// <param name="VerifyCopy">TRUE to perform testing to verify satisfactory copy (slows down return)</param>
			/// <returns>TRUE for success; FALSE for failure</returns>
			public bool CopyDirectory(string sourcePath, string storagePath, bool verifyCopy)
			{
				bool retCode = false;
				long locTreeSize = 0;
				long remTreeSize = 0;
				long locTreeFileCount = 0;
				long remTreeFileCount = 0;
				long dumDirCount = 0;

				//Verify FTP connection available
				if (!m_FtpDart.Connected)
				{
					m_ErrMsg = "FTP connection not open";
					return false;
				}

				try
				{
					FtpFile[] Results = null;
					m_FtpDart.FileType = FileType.Image;
					Results = m_FtpDart.Put(sourcePath, "*", storagePath, true, false,true);
					m_FtpDart.FileType = FileType.Ascii;
					retCode = ReportFTPResults(Results);
					if (!retCode)
					{
						m_ErrMsg = "Unable to copy dataset";
						return false;
					}
					//Verify successful copy
					if (verifyCopy)
					{
						//Get the number of files and total file size from the FTP server
						retCode = GetRemFileSizeCount(storagePath, ref remTreeSize, ref remTreeFileCount);
						if (!retCode)
						{
							m_ErrMsg = "Error verifying dataset copy: " + m_ErrMsg;
							return false;
						}
						//Get the number of files and total file size from the dataset folder
						locTreeSize = clsFileTools.GetDirectorySize(sourcePath, ref locTreeFileCount, ref dumDirCount);
						//Compare the file sizes and counts
						if ((locTreeSize != remTreeSize) | (locTreeFileCount != remTreeFileCount))
						{
							m_ErrMsg = "Error verifying dataset copy: " + m_ErrMsg;
							return false;
						}
					}
				}
				catch (Exception ex)
				{
					string msg = "clsFtpOperations.CopyDiretiry(): Exception copying directory";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, ex);
					m_ErrMsg = "ERROR: " + ex.Message.Replace(Environment.NewLine, " ");
					return false;
				}

				//Got to here, so everything's OK
				return true;
			}	// End sub

			/// <summary>
			/// Converts the incoming string from the FTP control into a file size
			/// </summary>
			/// <param name="FTPStr">A string of file information returned from an FTP server</param>
			/// <returns>The size of the file (bytes)</returns>
			private long InpStrToFileSize(string ftpStr)
			{
				//Code is based on example from "Tutorial 3: Retrieving FTP directory listing" at
				// http://www.vbip.com/protocols/ftp/vb-ftp-client-library/tutorials/tutorial-03.asp
				//
				//Requires function GetDateStartPosition from the same example

				int intDatePosition = 0;
				int intFileNamePosition = 0;
				int intFileSizePosition = 0;
				string TempStr = null;

				if (ftpStr.Length == 0) return 0;

				//Retrieve the start position of the file/directory date string
				intDatePosition = GetDateStartPosition(ftpStr);
				if (intDatePosition == -1) return 0;

				//Get the file name start position: intDatePosition + Len("Mar 21  1998") + 1
				intFileNamePosition = intDatePosition + 13;
				//
				//Remove CR symbols from the line
				ftpStr = ftpStr.Replace("\r", "");

				if (intDatePosition > 10)
				{
					//
					//Here is the "UNIX FTP listing format"
					//
					//Get the file size
					intFileSizePosition = ftpStr.LastIndexOf(" ", intDatePosition - 1);
					TempStr = ftpStr.Substring(intFileSizePosition, intDatePosition - intFileSizePosition - 1);
					return long.Parse(TempStr);
				}
				else
				{
					//
					//
					//Here is the "DOS FTP listing format"
					//
					if (ftpStr.Length > 0)
					{
						ftpStr = ftpStr.Substring(0, 38);
						if (ftpStr.IndexOf("<DIR>") > 0)
						{
							//					InpStrToFileSize = "0"
							return 0;
						}
						else
						{
							TempStr = ftpStr.Substring(18).Trim();
							return long.Parse(TempStr);
						}
					}
				}
				return 0;	// Shouldn't ever get here; this is just to make the compiler happy.
			}	// End sub

			/// <summary>
			/// Gets the starting position of the date portion in an FTP listing string
			/// Used by InpStrToFileSize
			/// </summary>
			/// <param name="strListingLine">A string of file information returned from an FTP server</param>
			/// <returns>Starting position of the date portion of the input string</returns>
			private int GetDateStartPosition(string strListingLine)
			{
				//Code is based on example from "Tutorial 3: Retrieving FTP directory listing" at
				// http://www.vbip.com/protocols/ftp/vb-ftp-client-library/tutorials/tutorial-03.asp
				//
				//
				//drwxr-xr-x   4 ftpuser  ftpusers       512 Jul  2  2001 kylix
				//                                          <------------>
				//drwxr-xr-x   3 ftpuser  ftpusers       512 Jul 19 11:25 optimizeit
				//                                          <------------>
				//
				int intStartPosition = 0;
				string[] MonthsArray = { "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", 
											    "nov", "dec" };
				string strStringToSearch = null;
				int i = 0;

				strListingLine = strListingLine.ToLower();

				try
				{
					for (i = 0; i <= MonthsArray.GetUpperBound(0); i++)
					{
						strStringToSearch = " " + MonthsArray[i] + " ";
						intStartPosition = strListingLine.IndexOf(strStringToSearch);
						if (intStartPosition > 0)
						{
							if (IsNumeric(strListingLine.Substring(intStartPosition - 1, 1)) 
									& IsNumeric(strListingLine.Substring(intStartPosition + 9, 1)))
							{
								//
								return intStartPosition + 1;
							}
						}
					}
					return 0;
				}
				catch (Exception ex)
				{
					return -1;
				}
			}	// End sub

			/// <summary>
			/// Gets a listing of all files in the top level of the specified folder on the FTP server
			/// </summary>
			/// <param name="RemFolder">Folder to get a listing from</param>
			/// <param name="WildCard">file spec</param>
			/// <returns>String collection containing the names of all files found</returns>
			public StringCollection GetRemFileList(string remFolder, string wildCard)
			{
				Listing remList = default(Listing);
				StringCollection tempCollection = new StringCollection();

				m_ErrMsg = "";

				//Verify FTP connection available
				if (!m_FtpDart.Connected)
				{
					m_ErrMsg = "FTP connection not open";
					return null;
				}

				//Get the listing
				try
				{
					remList = m_FtpDart.List(clsFileTools.CheckTerminator(remFolder, true, "/") + wildCard, true);
				}
				catch (Exception Err)
				{
					//Note: Err contains string containing "No files found" if no files
					m_ErrMsg = "Error retrieving listing: " + Err.Message;
					return null;
				}

				foreach (ListEntry MyItem in remList)
				{
					if (MyItem.Type == EntryType.File) tempCollection.Add(MyItem.Name);
				}

				return tempCollection;
			}	// End sub

			/// <summary>
			/// Retrieves the specified file from the FTP server
			/// </summary>
			/// <param name="SrcFile">Name and full path of file to retrieve</param>
			/// <param name="DestFile">Name and full path of file to retrieve to</param>
			/// <returns>TRUE for success; FALSE for failure</returns>
			public bool GetFile(string srcFile, string destFile)
			{
				FtpFile xferStat = null;

				//Verify FTP connection available
				if (!m_FtpDart.Connected)
				{
					m_ErrMsg = "FTP connection not open";
					return false;
				}

				try
				{
					//Retrieve the file
					m_FtpDart.FileType = FileType.Image;
					xferStat = m_FtpDart.Get(srcFile, destFile);
					m_FtpDart.FileType = FileType.Ascii;
					//Check for valid transfer
					if (xferStat.Status == FtpFileStatus.TransferCompleted)
					{
						return true;
					}
					else
					{
						m_ErrMsg = "Error during transfer, file " + srcFile;
						return false;
					}
				}
				catch (Exception Err)
				{
					m_ErrMsg = "Error retrieving file " + srcFile + "; " + Err.Message;
					return false;
				}
			}	// End sub

			/// <summary>
			/// Stores a single file in the archive
			/// </summary>
			/// <param name="SrcFile">Name and full path of file to store</param>
			/// <param name="DestFile">Name and full path of file in archive</param>
			/// <param name="Verify">TRUE to verify file sizes match after copy</param>
			/// <returns>TRUE for success; FALSE for failure</returns>
			public bool PutFile(string srcFile, string destFile, bool verify)
			{
				FtpFile xferStat = null;
				FileInfo locFileInfo = null;
				Listing remFileListing = null;

				//Verify FTP connection available
				if (!m_FtpDart.Connected)
				{
					m_ErrMsg = "FTP connection not open";
					return false;
				}

				try
				{
					//Copy the file to the archive
					m_FtpDart.FileType = FileType.Image;
					xferStat = m_FtpDart.Put(srcFile, destFile);
					m_FtpDart.FileType = FileType.Ascii;
					//Test for successful transfer
					if (xferStat.Status != FtpFileStatus.TransferCompleted)
					{
						m_ErrMsg = "Error during transfer, file " + srcFile;
						return false;
					}
				}
				catch (Exception Err)
				{
					m_ErrMsg = "Error storing file " + srcFile + "; " + Err.Message;
					return false;
				}

				if (verify)
				{
					//Verify successful copy of the file
					try
					{
						//Find the file on the destination
						remFileListing = m_FtpDart.List(destFile, true);
						if (remFileListing.Count != 1)
						{
							m_ErrMsg = "Error verifying file " + srcFile + "; Incorrect file count = " + remFileListing.Count.ToString();
							return false;
						}
						//Get the size of the local file
						locFileInfo = new FileInfo(srcFile);
						//Compare local file and remote file sizes
						if (remFileListing[0].Size != locFileInfo.Length)
						{
							m_ErrMsg = "File size mismatch, file " + srcFile;
							return false;
						}
						return true;
					}
					catch (Exception Err)
					{
						m_ErrMsg = "Error verifying file " + srcFile + "; " + Err.Message;
						return false;
					}
				}
				else
				{
					//Verification not required
					return true;
				}
			}	// End sub

			/// <summary>
			/// Checks for existence of specified folder on FTP server
			/// </summary>
			/// <param name="RemFolder">Name and full path of folder to be found</param>
			/// <returns>FileFolderFound enum value indicating if folder was found</returns>
			public EnumFileFolderExists FolderExists(string remFolder)
			{
				// Check is peformed by attempting to change remote directory to specified folder.
				//	If folder doesn't exist, an error occurs and function is set to False
				string tempFolder = "";

				m_ErrMsg = "";

				//Verify FTP connection available
				if (!m_FtpDart.Connected)
				{
					m_ErrMsg = "FTP connection not open";
					return EnumFileFolderExists.FindError;
				}

				remFolder = clsFileTools.CheckTerminator(remFolder, clsFileTools.TERMINATOR_REMOVE, "/");
				//Save the current working directory
				try
				{
					tempFolder = m_FtpDart.GetDirectory();
					m_FtpDart.Invoke(FtpCommand.ChangeDir, remFolder);
					//If there was no exception, then the directory exists.
					//	Return working directory to previous value and exit
					m_FtpDart.Invoke(FtpCommand.ChangeDir, tempFolder);
					return  EnumFileFolderExists.Found;
				}
				catch (ProtocolException Err)
				{
					if (Err.Message.IndexOf("No such file") > 0)
					{
						//Folder is not present in archive
						m_ErrMsg = "Directory not found";
						return EnumFileFolderExists.NotFound;
					}
					else
					{
						//Some other protocol error was returned from server
						m_ErrMsg = "Error finding directory: " + Err.Message;
						return EnumFileFolderExists.FindError;
					}
				}
				catch (Exception Err)
				{
					//Handle generic error
					m_ErrMsg = "Error finding directory: " + Err.Message;
					return EnumFileFolderExists.FindError;
				}
			}	// End sub

			/// <summary>
			/// Checks for existence of specified file on FTP server
			/// </summary>
			/// <param name="RemFile">Name and full path of file to be found</param>
			/// <returns>FileFolderFound enum value indicating if file was found</returns>
			public EnumFileFolderExists FileExists(string remFile)
			{
				Listing FolderData = null;

				m_ErrMsg = "";

				//Verify FTP connection available
				if (!m_FtpDart.Connected)
				{
					m_ErrMsg = "FTP connection not open";
					return EnumFileFolderExists.FindError;
				}

				//Get a listing from the FTP server
				try
				{
					FolderData = m_FtpDart.List(remFile, true);
				}
				catch (Exception ex)
				{
					m_ErrMsg = "Error getting listing from server: " + ex.Message;
					return EnumFileFolderExists.FindError;
				}
				//Check the number of items found to determine if file was found
				if (FolderData.Count < 1)
				{
					m_ErrMsg = "File not found";
					return EnumFileFolderExists.NotFound;
				}
				else if (FolderData.Count > 1)
				{
					m_ErrMsg = "Too many files found";
					return EnumFileFolderExists.FindError;
				}
				else
				{
					return EnumFileFolderExists.Found;
				}
			}	// End sub

			/// <summary>
			/// Renames an existing file in the archive
			/// </summary>
			/// <param name="OldName">Full path and original file name</param>
			/// <param name="NewName">Full path and new file name</param>
			/// <returns>TRUE for success; FALSE for failure</returns>
			public bool RenameFile(string OldName, string NewName)
			{
				//Verify FTP connection available
				if (!m_FtpDart.Connected)
				{
					m_ErrMsg = "FTP connection not open";
					return false;
				}

				//Perform the rename operation
				try
				{
					m_FtpDart.Rename(OldName, NewName);
					return true;
				}
				catch (Exception ex)
				{
					m_ErrMsg = "Exception renaming file " + OldName + ": " + ex.Message;
					return false;
				}
			}	// Ens sub

			/// <summary>
			/// Gets the total file count and size for a remote folder
			/// </summary>
			/// <param name="RemoteFolder">Name of remote folder</param>
			/// <param name="TotSize">Return value for total size of files (bytes)</param>
			/// <param name="TotCount">Return value for total number of files found</param>
			/// <returns>TRUE for success; FALSE for failure</returns>
			private bool GetRemFileSizeCount(string remoteFolder, ref long totSize, ref long totCount)
			{
				System.Collections.Stack DirStack = null;
				long TempFileCount = 0;
				long TempFileSize = 0;
				Listing ItemList = default(Listing);
				string CurRemDir = null;

				//Verify FTP connection available
				if (!m_FtpDart.Connected)
				{
					m_ErrMsg = "FTP connection not open";
					return false;
				}

				m_FtpDart.FileType = FileType.Ascii;

				//Set up a stack that initially holds 20 directories
				DirStack = new System.Collections.Stack(20);

				//Add the current directory to the stack
				DirStack.Push(remoteFolder);

				//Loop through the stack until no more directories are found, counting all the way
				while (DirStack.Count > 0)
				{
					//Pop the top entry
					CurRemDir = (string)DirStack.Pop();
					//Get a list of all objects in the directory
					try
					{
						m_FtpDart.Invoke(FtpCommand.ChangeDir, CurRemDir);
						ItemList = m_FtpDart.List("", true);
					}
					catch (Exception Err)
					{
						m_ErrMsg = "Error retrieving directory list: " + CurRemDir + ", Error: " + Err.Message;
						return false;
					}
					//Handle to the objects that resulted from the listing operation
					foreach (ListEntry RemListItem in ItemList)
					{
						if (RemListItem.Type == EntryType.File)
						{
							//This is a file, so count it
							TempFileSize += RemListItem.Size;
							TempFileCount += 1;
						}
						else if (RemListItem.Type == EntryType.Directory)
						{
							DirStack.Push(clsFileTools.CheckTerminator(CurRemDir, true, "/") + RemListItem.Name);
						}
					}
				}

				//All finished, so set return values and clean up
				totCount = TempFileCount;
				totSize = TempFileSize;
				DirStack = null;

				return true;
			}	// End sub

			/// <summary>
			/// Examine results of ftp operation and report errors
			/// </summary>
			/// <param name="Results">A Dart FtpFile object containing FTP file operation results</param>
			/// <returns>TRUE if operation resulted in no errors; otherwise FALSE</returns>
			private bool ReportFTPResults(FtpFile[] results)
			{
				if (results.GetLength(0) == 0)
				{
					m_ErrMsg = "No files transferred.";
					return false;
				}
				else
				{
					// Check each result to see if there were any errors
					foreach (FtpFile f in results)
					{
						// If the exception property is true, display the error message
						if ((f.Exception != null))
						{
							m_ErrMsg = "File xfer error (" + f.RemoteFileName + "): " + 
										f.Exception.Message.Replace(Environment.NewLine, " ");
							return false;
						}
						else if (f.Count == -1)
						{
							// If Count is -1, it means that AbortTransfer was called
							// before this file started transferring
							m_ErrMsg = "File xfer aborted, file " + f.RemoteFileName;
							return false;
						}
					}
					m_ErrMsg = "Succesfully retrieved " + results.Length.ToString() + " files.";
					return true;
				}
			}	// End sub

			/// <summary>
			/// Decrypts password received from ini file
			/// </summary>
			/// <param name="EnPwd">Encoded password</param>
			/// <returns>Clear text password</returns>
			private string DecodePassword(string enPwd)
			{
				// Decrypts password received from ini file
				// Password was created by alternately subtracting or adding 1 to the ASCII value of each character

				// Convert the password string to a character array
				char[] pwdChars = enPwd.ToCharArray();
				byte[] pwdBytes = new byte[pwdChars.Length];
				char[] pwdCharsAdj = new char[pwdChars.Length];

				for (int i = 0; i < pwdChars.Length; i++)
				{
					pwdBytes[i] = (byte)pwdChars[i];
				}

				// Modify the byte array by shifting alternating bytes up or down and convert back to char, and add to output string
				string retStr = "";
				for (int byteCntr = 0; byteCntr < pwdBytes.Length; byteCntr++)
				{
					if ((byteCntr % 2) == 0)
					{
						pwdBytes[byteCntr] += 1;
					}
					else
					{
						pwdBytes[byteCntr] -= 1;
					}
					pwdCharsAdj[byteCntr] = (char)pwdBytes[byteCntr];
					retStr += pwdCharsAdj[byteCntr].ToString();
				}
				return retStr;
			}	// End sub

			/// <summary>
			/// Replaces the password returned by the Dart FTP control error message with "xxxxx" so the password doesn't appear in log files
			/// </summary>
			/// <param name="InpStr">Input string from error message</param>
			/// <returns>input string, only password is replaced with "xxxxxx"</returns>
			private string StripPwd(string inpStr)
			{
				//Replaces the password returned by the Dart FTP control error message with "xxxxx"
				return System.Text.RegularExpressions.Regex.Replace(inpStr, "PASS \\w*", "PASS xxxxxxx");
			}	// End sub

			/// <summary>
			/// Tests a string to determine if it can be converted to a numeric value
			/// </summary>
			/// <param name="inpStr">String to test</param>
			/// <returns>TRUE if can be converted, otherwise FALSE</returns>
			private bool IsNumeric(string inpStr)
			{
				long tmpLong = 0;
				float tmpFloat = 0F;

				// First, see if it can be a long (or integer)
				if (long.TryParse(inpStr,out tmpLong)) return true;	// It can be converted to long or integer

				// Now try it as a float
				if (float.TryParse(inpStr, out tmpFloat)) return true;	// It can be converted to a float

				// If we got to here, the string isn't a numeric type
				return false;
			}	// End sub
		#endregion

		#region "Event handlers"
			/// <summary>
			/// Create a stream to use for a log file for this session; 
			/// Write the info to the end of the stream, then close the stream
			/// </summary>
			/// <param name="sender"></param>
			/// <param name="e"></param>
			/// <remarks></remarks>
			private void On_FPT_Dart_Trace(object sender, SegmentEventArgs e)
			{
				//TODO: Figure out where to put the event assignment line for this
				string DumStr = null;
				int DumPos = 0;

				if (m_LogFile)
				{
					string CurDate = System.DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.ff");
					string Entry = "";
					if (e.Segment.Sent)
					{
						DumStr = e.Segment.ToString();
						DumPos = DumStr.IndexOf("PASS ");
						if (DumPos != -1)
						{
							//Strip off password
							DumStr = DumStr.Substring(0, DumPos + 4) + " XXXX" + System.Environment.NewLine;
						}
						Entry += System.Environment.NewLine + CurDate + " ---> " + DumStr;
					}
					else
					{
						Entry += System.Environment.NewLine + CurDate + " <--- " + e.Segment.ToString();
					}
					long Retry = 0;
					Retry = 10;
					while (Retry > 0)
					{
						try
						{
							FileStream Stream = new FileStream(Application.StartupPath + "\\ArchiveFTP.log", FileMode.OpenOrCreate, FileAccess.ReadWrite);
							Stream.Seek(0, SeekOrigin.End);
							Stream.Write(System.Text.ASCIIEncoding.ASCII.GetBytes(Entry.ToCharArray()), 0, 
												System.Text.ASCIIEncoding.ASCII.GetByteCount(Entry.ToCharArray()));
							Stream.Close();
							Retry = 0;
						}
						catch (Exception ex)
						{
							Retry -= 1;
							if (Retry == 0) return;
						}
					}
				}
			}	// End sub
		#endregion
	}	// End class
}	// End namespace
