
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 10/20/2009
//
// Last modified 10/20/2009
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
			/// Performs an archive task (oeverides base)
			/// </summary>
			/// <returns>TRUE for success, FALSE for failure</returns>
			public override bool PerformTask()
			{
				// TODO: Add job duration stuff?

				// Perform base class operations
				if (!base.PerformTask()) return false;

				m_Msg = "Archiving dataset " + m_TaskParams.GetParam("dataset");
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.INFO, m_Msg);

				if (!CopyOneFolderToArchive(m_DSNamePath, m_ArchiveNamePath)) return false;

				// Got to here, everything's wonderful!
				m_Msg = "Archive complete, dataset " + m_TaskParams.GetParam("dataset");
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_Msg);

				return true;
			}	// End sub
		#endregion
	}	// End class
}	// End namespace
