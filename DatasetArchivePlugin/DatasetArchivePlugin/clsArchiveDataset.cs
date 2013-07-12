
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
using CaptureTaskManager;

namespace DatasetArchivePlugin
{
	class clsArchiveDataset : clsOpsBase
	{
		//*********************************************************************************************************
		// Tools to archive a new dataset (entire dataset folder)
		//**********************************************************************************************************

		#region "Class variables"
		#endregion

		#region "Constructors"
			/// <summary>
			/// Constructor
			/// </summary>
			/// <param name="MgrParams">Manager parameters</param>
			/// <param name="TaskParams">Task parameters</param>
			public clsArchiveDataset(IMgrParams MgrParams, ITaskParams TaskParams)
				: base(MgrParams, TaskParams)
			{
			}	// End sub
		#endregion

		#region "Methods"
			/// <summary>
			/// Performs an archive task (overrides base)
			/// </summary>
			/// <returns>TRUE for success, FALSE for failure</returns>
			public override bool PerformTask()
			{
				bool copySuccess;
				bool stageSuccess;

				// Perform base class operations
				if (!base.PerformTask()) return false;

				m_Msg = "Archiving dataset " + m_TaskParams.GetParam("dataset") + ", job " + m_TaskParams.GetParam("Job");
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.INFO, m_Msg);

				// Determine the total size of the source folder
				// If over 5 GB, then we should use the Archive Update mechanism which will support retrying the archive
				float folderSizeGB = ComputeFolderSizeGB(m_DSNamePath);

				if (folderSizeGB >= 5)
				{

					m_Msg = "Dataset " + m_TaskParams.GetParam("dataset")  + " is " + folderSizeGB.ToString("0") + " GB; will use the ArchiveUpdate mechanism since this supports resuming an interrupted FTP operation";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.INFO, m_Msg);

					clsArchiveUpdate oArchiveUpdate = new clsArchiveUpdate(m_MgrParams, m_TaskParams);
					oArchiveUpdate.CreateDatasetFolderInArchiveIfMissing = true;

					copySuccess = oArchiveUpdate.PerformTask();
					return copySuccess;
				}
				else
				{
					copySuccess = CopyOneFolderToArchive(m_DSNamePath, m_ArchiveNamePath);
				}

				// Set the path to the results folder in archive
				string sResultsFolderPathArchive = System.IO.Path.Combine(m_TaskParams.GetParam("Archive_Network_Share_Path"), m_TaskParams.GetParam("folder"));

				if (!copySuccess)
				{
					// If the folder was created in the archive then do not exit this function yet; we need to call CreateMD5StagingFile
					if (!System.IO.Directory.Exists(sResultsFolderPathArchive))
						return false;
				}

				// Create a new stagemd5 file for each file m_DSNamePath
				stageSuccess = CreateMD5StagingFile(m_DSNamePath, sResultsFolderPathArchive);
				if (!stageSuccess)
				{
					if (string.IsNullOrEmpty(m_ErrMsg))
						m_ErrMsg = "Error creating the MD5 staging file";

					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_ErrMsg + " for " + m_DatasetName);

					// Temporarily ignoring, effective January 31, 2013 at 10:45 pm
					// return false;
					m_WarningMsg = "CreateMD5StagingFile returned false";
				}

				if (!copySuccess)
				{
					if (string.IsNullOrEmpty(m_ErrMsg))
						m_ErrMsg = "Error copying folder to the archive";

					return false;
				}

				// Got to here, everything's wonderful!
				m_Msg = "Archive complete, dataset " + m_TaskParams.GetParam("dataset") + ", job " + m_TaskParams.GetParam("Job");
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_Msg);

				return true;
			}	// End sub
		#endregion
	}	// End class
}	// End namespace
