//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2011, Battelle Memorial Institute
// Created 03/07/2011
//
// Last modified 03/07/2011
//               04/22/2011 (DAC) - Modified to use "real" demultiplexing dll's
//*********************************************************************************************************
using System;
using System.IO;
using CaptureTaskManager;
using UIMFDemultiplexer;
using FileProcessor;
using System.Threading;

namespace ImsDemuxPlugin
{
	public class clsDemuxTools
	{
		//*********************************************************************************************************
		//Insert general class description here
        //**********************************************************************************************************

        #region "Constants"
            protected const string DECODED_UIMF_SUFFIX = "_decoded.uimf";

            protected const int MAX_CHECKPOINT_FRAME_INTERVAL = 50;
            protected const int MAX_CHECKPOINT_WRITE_FREQUENCY_MINUTES = 20;

        #endregion

        #region "Module variables"
            UIMFDemultiplexer.UIMFDemultiplexer m_DeMuxTool;

            bool m_OutOfMemoryException = false;
            string m_DatasetFolderPathRemote = string.Empty;

		#endregion

		#region "Events"
			// Events used for communication back to clsPluginMain, where the logging and status updates are handled
			//public event DelDemuxErrorHandler DemuxError;
			//public event DelDemuxMessageHandler DumuxMsg;
			//public event DelDumuxExceptionHandler DemuxException;
			public event DelDemuxProgressHandler DemuxProgress;
		#endregion

        #region "Properties"
            public bool OutOfMemoryException
            {
                get { return m_OutOfMemoryException; }
            }
        #endregion

            #region "Constructor"
            public clsDemuxTools()
			{
				m_DeMuxTool = new UIMFDemultiplexer.UIMFDemultiplexer();
                m_DeMuxTool.ErrorEvent += new clsProcessFilesBaseClass.MessageEventHandler(deMuxTool_ErrorEventHandler);
                m_DeMuxTool.WarningEvent += new clsProcessFilesBaseClass.MessageEventHandler(deMuxTool_WarningEventHandler);
				m_DeMuxTool.MessageEvent += new clsProcessFilesBaseClass.MessageEventHandler(deMuxTool_MessageEventHandler);
			}
		#endregion

		#region "Methods"
			/// <summary>
			/// Performs de-multiplexing of IMS data files
			/// </summary>
			/// <param name="mgrParams">Parameters for manager operation</param>
			/// <param name="taskParams">Parameters for the assigned task</param>
			/// <returns>Enum indicating task success or failure</returns>
            public clsToolReturnData PerformDemux(IMgrParams mgrParams, ITaskParams taskParams, string uimfFileName)
			{
				string msg = "Performing de-multiplexing, dataset " + taskParams.GetParam("Dataset");
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

				clsToolReturnData retData = new clsToolReturnData();

				string dataset = taskParams.GetParam("Dataset");
                bool bPostProcessingError = false;

                // Make sure the working directory is empty
                string workDirPath = mgrParams.GetParam("workdir");
                ClearWorkingDirectory(workDirPath);

				// Locate data file on storage server

				string svrPath = Path.Combine(taskParams.GetParam("Storage_Vol_External"), taskParams.GetParam("Storage_Path"));
                m_DatasetFolderPathRemote = Path.Combine(svrPath, taskParams.GetParam("Folder"));

                string uimfRemoteEncodedFileNamePath = Path.Combine(m_DatasetFolderPathRemote, uimfFileName);
				string uimfLocalEncodedFileNamePath = Path.Combine(workDirPath, dataset + ".uimf");
                
				// Copy uimf file to working directory
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Copying file from storage server");
                int retryCount = 0;
                if (!CopyFile(uimfRemoteEncodedFileNamePath, uimfLocalEncodedFileNamePath, false, retryCount))
				{
					retData.CloseoutMsg = "Error copying UIMF file to working directory";
					retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
					return retData;
				}

                // Look for a _decoded.uimf.tmp file on the storage server
                // Copy it local if present
                string sTmpUIMFFileName = dataset + DECODED_UIMF_SUFFIX + ".tmp";
                string sTmpUIMFRemoteFileNamePath = Path.Combine(m_DatasetFolderPathRemote, sTmpUIMFFileName);
                string sTmpUIMFLocalFileNamePath = Path.Combine(workDirPath, sTmpUIMFFileName);
                
                bool bResumeDemultiplexing = false;
                int iResumeStartFrame = 0;

                if (System.IO.File.Exists(sTmpUIMFRemoteFileNamePath))
                {
                    // Copy uimf.tmp file to working directory
                    retryCount = 0;
                    if (CopyFile(sTmpUIMFRemoteFileNamePath, sTmpUIMFLocalFileNamePath, false, retryCount))
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, ".tmp decoded file found at " + sTmpUIMFRemoteFileNamePath + "; will resume demultiplexing");
                        bResumeDemultiplexing = true;
                    }
                    else
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Error copying .tmp decoded file from " + sTmpUIMFRemoteFileNamePath + " to work folder; unable to resume demultiplexing");
                    }

                }

				// Perform demux operation
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Calling demux dll");

                try
                {
                    if (!DemultiplexFile(uimfLocalEncodedFileNamePath, dataset, bResumeDemultiplexing, out iResumeStartFrame))
                    {
                        retData.CloseoutMsg = "Error demultiplexing UIMF file";
                        retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                        return retData;
                    }
                }
                catch (Exception ex)
                {
                    msg = "Exception calling DemultiplexFile for dataset " + dataset;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, ex);
                    retData.CloseoutMsg = "Error demultiplexing UIMF file";
                    retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    return retData;
                }


			    // Look for the demultiplexed .UIMF file
				string localUimfDecodedFilePath = Path.Combine(workDirPath, dataset + DECODED_UIMF_SUFFIX);

                if (!System.IO.File.Exists(localUimfDecodedFilePath))
                {
                    retData.CloseoutMsg = "Decoded UIMF file not found";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, retData.CloseoutMsg + ": " + localUimfDecodedFilePath);
                    retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    return retData;
                }

                if (!ValidateUIMFLogEntries(localUimfDecodedFilePath))
                    bPostProcessingError = true;


                if (!bPostProcessingError)
                {
                    // Rename uimf file on storage server
                    msg = "Renaming uimf file on storage server";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
                    // If this is a re-run, then encoded file has already been renamed
                    // This is determined by looking for "encoded" in uimf file name
                    if (!uimfFileName.Contains("encoded"))
                    {
                        if (!RenameFile(uimfRemoteEncodedFileNamePath, Path.Combine(m_DatasetFolderPathRemote, dataset + "_encoded.uimf")))
                        {
                            retData.CloseoutMsg = "Error renaming encoded UIMF file on storage server";
                            retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                            bPostProcessingError = true;
                        }
                    }
                }

                if (!bPostProcessingError)
                {
                    // Delete CheckPoint file from storage server (if it exists)
                    if (!string.IsNullOrEmpty(m_DatasetFolderPathRemote))
                    {
                        msg = "Deleting .uimf.tmp CheckPoint file from storage server";
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

                        try
                        {
                            string sCheckpointTargetPath = System.IO.Path.Combine(m_DatasetFolderPathRemote, sTmpUIMFFileName);

                            if (System.IO.File.Exists(sCheckpointTargetPath))
                                System.IO.File.Delete(sCheckpointTargetPath);
                        }
                        catch (Exception ex)
                        {
                            msg = "Error deleting .uimf.tmp CheckPoint file: " + ex.Message;
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                        }

                    }
                }

                if (!bPostProcessingError)
                {
                    // Copy demuxed file to storage server, renaming as datasetname.uimf in the process
                    msg = "Copying de-mulitiplexed file to storage server";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
                    retryCount = 3;
                    if (!CopyFile(localUimfDecodedFilePath, Path.Combine(m_DatasetFolderPathRemote, dataset + ".uimf"), true, retryCount))
                    {
                        retData.CloseoutMsg = "Error copying decoded UIMF file to storage server";
                        retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                        bPostProcessingError = true;
                    }
                }

                if (bPostProcessingError)
                {
                    try
                    {
                        // Delete the multiplexed .UIMF file (no point in saving it)
                        File.Delete(uimfLocalEncodedFileNamePath);
                    }
                    catch
                    {
                        // Ignore errors deleting the multiplexed .UIMF file
                    }
                    
                    // Try to save the demultiplexed .UIMF file (and any other files in the work directory)
                    clsFailedResultsCopier oFailedResultsCopier = new clsFailedResultsCopier(mgrParams, taskParams);
                    oFailedResultsCopier.CopyFailedResultsToArchiveFolder(workDirPath);
                    oFailedResultsCopier = null;

                    return retData;
                }

				// Delete local uimf file(s)
				msg = "Cleaning up working directory";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
				try
				{
					File.Delete(localUimfDecodedFilePath);
					File.Delete(uimfLocalEncodedFileNamePath);
				}
				catch (Exception ex)
				{
                    // Error deleting files; don't treat this as a fatal error
					msg = "Exception deleting working directory file(s): " + ex.Message;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
				}

				retData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
				retData.EvalMsg = "De-multiplexed";
                if (bResumeDemultiplexing)
                    retData.EvalMsg += " (resumed at frame " + iResumeStartFrame + ")";

				return retData;

			}	// End sub

            /// <summary>
            /// Examines the Log_Entries table in the UIMF file to make sure the expected log entries are present
            /// </summary>
            /// <param name="localUimfDecodedFilePath"></param>
            /// <returns></returns>
            private bool ValidateUIMFLogEntries(string localUimfDecodedFilePath)
            {
                bool bUIMFIsValid = true;
                string msg;

                // Make sure the Log_Entries table contains entry "Finished demultiplexing" (with today's date)
                UIMFDemultiplexer.clsUIMFLogEntryAccessor oUIMFLogEntryAccessor = new UIMFDemultiplexer.clsUIMFLogEntryAccessor();
                DateTime dtDemultiplexingFinished;
                string sLogEntryAccessorMsg;

                dtDemultiplexingFinished = oUIMFLogEntryAccessor.GetDemultiplexingFinishDate(localUimfDecodedFilePath, out sLogEntryAccessorMsg);

                if (dtDemultiplexingFinished == System.DateTime.MinValue)
                {
                    msg = "Demultiplexing finished message not found in Log_Entries table in " + localUimfDecodedFilePath;
                    if (!String.IsNullOrEmpty(sLogEntryAccessorMsg))
                        msg += "; " + sLogEntryAccessorMsg;

                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                    bUIMFIsValid = false;
                }
                else
                {
                    if (System.DateTime.Now.Subtract(dtDemultiplexingFinished).TotalMinutes < 5)
                    {
                        msg = "Demultiplexing finished message in Log_Entries table has date " + dtDemultiplexingFinished.ToString();
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
                        bUIMFIsValid = true;
                    }
                    else
                    {
                        msg = "Demultiplexing finished message in Log_Entries table is more than 5 minutes old: " + dtDemultiplexingFinished.ToString() + "; assuming this is a demultiplexing failure";
                        if (!String.IsNullOrEmpty(sLogEntryAccessorMsg))
                            msg += "; " + sLogEntryAccessorMsg;

                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                        bUIMFIsValid = false;
                    }
                }

                if (bUIMFIsValid)
                {
                    // Make sure the Log_Entries table contains entry "Applied calibration coefficients to all frames" (with today's date)
                    DateTime dtCalibrationApplied;
                    sLogEntryAccessorMsg = string.Empty;

                    dtCalibrationApplied = oUIMFLogEntryAccessor.GetCalibrationFinishDate(localUimfDecodedFilePath, out sLogEntryAccessorMsg);

                    if (dtCalibrationApplied == System.DateTime.MinValue)
                    {
                        msg = "Applied calibration message not found in Log_Entries table in " + localUimfDecodedFilePath;
                        if (!String.IsNullOrEmpty(sLogEntryAccessorMsg))
                            msg += "; " + sLogEntryAccessorMsg;

                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                        bUIMFIsValid = false;
                    }
                    else
                    {
                        if (System.DateTime.Now.Subtract(dtCalibrationApplied).TotalMinutes < 5)
                        {
                            msg = "Applied calibration message in Log_Entries table has date " + dtCalibrationApplied.ToString();
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
                            bUIMFIsValid = true;
                        }
                        else
                        {
                            msg = "Applied calibrationmessage in Log_Entries table is more than 5 minutes old: " + dtCalibrationApplied.ToString() + "; assuming this is a demultiplexing failure";
                            if (!String.IsNullOrEmpty(sLogEntryAccessorMsg))
                                msg += "; " + sLogEntryAccessorMsg;

                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                            bUIMFIsValid = false;
                        }
                    }
                }

                return bUIMFIsValid;
            }

            /// <summary>
            /// Makes sure the working directory is empty
            /// </summary>
            /// <param name="sWorkingDirectory"></param>
            private void ClearWorkingDirectory(string sWorkingDirectory)
            {
                System.IO.DirectoryInfo diWorkDir = new System.IO.DirectoryInfo(sWorkingDirectory);

                if (diWorkDir.Exists)
                {
                    foreach (System.IO.FileInfo fiFile in diWorkDir.GetFiles())
                    {
                        try
                        {
                            fiFile.Delete();
                        }
                        catch (Exception ex)
                        {
					        string msg = "Exception deleting file '" + fiFile.Name + "' from work directory";
					        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, ex);
                        }
                        
                    }

                    foreach (System.IO.DirectoryInfo diSubFolder in diWorkDir.GetDirectories())
                    {
                        try
                        {
                            diSubFolder.Delete(true);
                        }
                        catch (Exception ex)
                        {
                            string msg = "Exception deleting folder '" + diSubFolder.Name + "' from work directory";
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, ex);
                        }
                    }
                }
                
            }

			/// <summary>
			/// Performs actual de-multiplexing operation in a separate thread
			/// </summary>
			/// <param name="inputFile">Input file name</param>
            /// <param name="datasetName">Dataset name</param>
			/// <returns>Enum indicating success or failure</returns>
            private bool DemultiplexFile(string inputFilePath, string datasetName, bool bResumeDemultiplexing, out int iResumeStartFrame)
			{
                const int STATUS_DELAY_MSEC = 5000;

                string msg;
                string sLogEntryAccessorMsg;
				bool success = false;
                iResumeStartFrame = 0;

                UIMFDemultiplexer.clsUIMFLogEntryAccessor oUIMFLogEntryAccessor = new UIMFDemultiplexer.clsUIMFLogEntryAccessor();

				FileInfo fi = new FileInfo(inputFilePath);
				string folderName = fi.DirectoryName;
                string outputFilePath = Path.Combine(folderName, datasetName + DECODED_UIMF_SUFFIX);

				try
				{
                    m_OutOfMemoryException = false;

                    if (bResumeDemultiplexing)
                    {
                        string sTempUIMFFilePath = outputFilePath + ".tmp";
                        if (!System.IO.File.Exists(sTempUIMFFilePath))
                        {
                            msg = "Resuming demultiplexing, but .tmp UIMF file not found at " + sTempUIMFFilePath;
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                            m_DeMuxTool.ResumeDemultiplexing = false;
                        }
                        else
                        {

                            int iMaxDemultiplexedFrameNum = oUIMFLogEntryAccessor.GetMaxDemultiplexedFrame(sTempUIMFFilePath, out sLogEntryAccessorMsg);
                            if (iMaxDemultiplexedFrameNum > 0)
                            {
                                iResumeStartFrame = iMaxDemultiplexedFrameNum + 1;
                                m_DeMuxTool.ResumeDemultiplexing = true;
                                msg = "Resuming de-multiplexing, dataset " + datasetName + " frame " + iResumeStartFrame;
                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
                            }
                            else
                            {
                                msg = "Error looking up max demultiplexed frame number from the Log_Entries table in " + sTempUIMFFilePath;
                                if (!String.IsNullOrEmpty(sLogEntryAccessorMsg))
                                    msg += "; " + sLogEntryAccessorMsg;

                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);

                                m_DeMuxTool.ResumeDemultiplexing = false;
                            }
                        }
                    }
                    else
                    {
                        msg = "Starting de-multiplexing, dataset " + datasetName;
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
                        m_DeMuxTool.ResumeDemultiplexing = false;
                    }

                    // Enable checkpoint file creation
                    m_DeMuxTool.CreateCheckpointFiles = true;
                    m_DeMuxTool.CheckpointFrameIntervalMax = MAX_CHECKPOINT_FRAME_INTERVAL;
                    m_DeMuxTool.CheckpointWriteFrequencyMinutesMax = MAX_CHECKPOINT_WRITE_FREQUENCY_MINUTES;
                    m_DeMuxTool.CheckpointTargetFolder = m_DatasetFolderPathRemote;

                    /*
                     * Old code that ran the demultiplexer on a separate thread
                     * Fails to catch and log exceptions thrown by UIMFDemultiplexer.dll
                     * 
                        // Create a thread to run the demuxer
                        Thread demuxThread;
                        demuxThread = new Thread(new ThreadStart(() => m_DeMuxTool.Demultiplex(inputFile, outputFilePath)));

                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Starting Demux Thread");

                        // Start the demux thread
                        demuxThread.Start();

                        // Wait until the thread completes
                        //TODO: Does this need a way to abort?
                        while (demuxThread != null && !demuxThread.Join(STATUS_DELAY_MSEC))
                        {
                            if (DemuxProgress != null) DemuxProgress(m_DeMuxTool.ProgressPercentComplete);
                        }

                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Demux thread completed");
                        success = true;
                    */
                    
                    // Create a timer that will be used to log progress
                    System.Threading.Timer tmrUpdateProgress;
                    tmrUpdateProgress = new System.Threading.Timer(new TimerCallback(timer_ElapsedEvent));
                    tmrUpdateProgress.Change(STATUS_DELAY_MSEC, STATUS_DELAY_MSEC);

                    success = m_DeMuxTool.Demultiplex(inputFilePath, outputFilePath);
                

					// Check to determine if thread exited due to normal completion
                    if (success && m_DeMuxTool != null && 
                        m_DeMuxTool.ProcessingStatus == UIMFDemultiplexer.UIMFDemultiplexer.eProcessingStatus.Complete &&
                        !m_OutOfMemoryException)
					{
						msg = "De-multiplexing complete, dataset " + datasetName;
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
                        success = true;                        
					}
					else
					{
                        string errorMsg = "Unknown error";
                        if (m_OutOfMemoryException)
                            errorMsg = "OutOfMemory exception was thrown";                        

						// Log the processing status
                        if (m_DeMuxTool != null)
                        {
                            msg = "Demux processing status: " + m_DeMuxTool.ProcessingStatus.ToString();
                            
                            // Get the error msg
                            errorMsg = m_DeMuxTool.GetErrorMessage();
                            if (string.IsNullOrEmpty(errorMsg))
                            {
                                errorMsg = "Unknown error";
                                if (m_OutOfMemoryException)
                                    errorMsg = "OutOfMemory exception was thrown";                        
                            }

                        }
                        else
                        {
                            msg = "Demux processing status: ??? (m_DeMuxTool is null)";
                        }

						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errorMsg);
						success = false;
					}
				}
				catch (Exception ex)
				{
					msg = "Exception de-multiplexing dataset " + datasetName;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, ex);
					success = false;
				}

				return success;
			}	// End sub

			/// <summary>
			/// Copies a file
			/// </summary>
			/// <param name="sourceFileNamePath">Source file</param>
			/// <param name="TargetFileNamePath">Destination file</param>
			/// <returns></returns>
			private bool CopyFile(string sourceFileNamePath, string TargetFileNamePath, bool overWrite, int retryCount)
			{
                bool bRetryingCopy = false;
                string msg;

                if (retryCount < 0)
                    retryCount = 0;

                while (retryCount >= 0)
                {
                    try
                    {
                        if (bRetryingCopy)
                        {
                            msg = "Retrying copy; retryCount = " + retryCount;
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
                        }

                        File.Copy(sourceFileNamePath, TargetFileNamePath, overWrite);
                        return true;
                    }
                    catch (Exception ex)
                    {
                        msg = "Exception copying file " + sourceFileNamePath + " to " + TargetFileNamePath  + ": " + ex.Message;
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);

                        System.Threading.Thread.Sleep(2000);
                        retryCount -= 1;
                        bRetryingCopy = true;
                    }
                 
                }

                // If we get here, then we were not able to successfully copy the file
                return false;

			}	// End sub

			/// <summary>
			/// Renames a file
			/// </summary>
			/// <param name="currFileNamePath">Original file name and path</param>
			/// <param name="newFileNamePath">New file name and path</param>
			/// <returns></returns>
			private bool RenameFile(string currFileNamePath, string newFileNamePath)
			{
				try
				{
					FileInfo fi = new FileInfo(currFileNamePath);
					fi.MoveTo(newFileNamePath);
					return true;
				}
				catch (Exception ex)
				{
					string msg = "Exception renaming file " + currFileNamePath + ": " + ex.Message;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
					return false;
				}
			}	// End sub
		#endregion

		#region "Event handlers"
			/// <summary>
			/// Logs a message from the demux dll
			/// </summary>
			/// <param name="sender"></param>
			/// <param name="e"></param>
            void deMuxTool_MessageEventHandler(object sender, MessageEventArgs e)
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Demux message: " + e.Message);
			}

			/// <summary>
			/// Logs a warning from the demux dll
			/// </summary>
			/// <param name="sender"></param>
			/// <param name="e"></param>
            void deMuxTool_WarningEventHandler(object sender, MessageEventArgs e)
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Demux warning: " + e.Message);
			}

			/// <summary>
			/// Logs an error from the debug dll
			/// </summary>
			/// <param name="sender"></param>
			/// <param name="e"></param>
            void deMuxTool_ErrorEventHandler(object sender, MessageEventArgs e)
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Demux error: " + e.Message);
                if (e.Message.Contains("OutOfMemoryException"))
                    m_OutOfMemoryException = true;
			}
               
            void timer_ElapsedEvent(object stateInfo)
            {
                // Update the status if it has changed since the last call
                if (DemuxProgress != null) 
                    DemuxProgress(m_DeMuxTool.ProgressPercentComplete);

            }

		#endregion
	}	// End class
}	// End namespace
