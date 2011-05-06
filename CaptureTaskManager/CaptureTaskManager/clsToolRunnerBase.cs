
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 09/25/2009
//
// Last modified 09/25/2009
//*********************************************************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CaptureTaskManager
{
	public class clsToolRunnerBase : IToolRunner
	{
		//*********************************************************************************************************
		// Base class for capture step tool plugins
		//**********************************************************************************************************

		#region "Constants"
		#endregion

		#region "Class variables"
			protected IMgrParams m_MgrParams;
			protected ITaskParams m_TaskParams;
			protected IStatusFile m_StatusTools;

            protected System.DateTime m_LastConfigDBUpdate = System.DateTime.Now;
            protected int m_MinutesBetweenConfigDBUpdates = 10;
		#endregion

		#region "Delegates"
		#endregion

		#region "Events"
		#endregion

		#region "Properties"
		#endregion

		#region "Constructors"
			/// <summary>
			/// Constructor
			/// </summary>
			protected clsToolRunnerBase()
			{
				// Does nothing at present
			}	// End sub
		#endregion

		#region "Methods"
			/// <summary>
			/// Runs the plugin tool. Implements IToolRunner.RunTool method
			/// </summary>
			/// <returns>Enum indicating success or failure</returns>
			public virtual clsToolReturnData RunTool()
			{
				// Does nothing at present, so return success
				clsToolReturnData retData = new clsToolReturnData();
				retData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
				return retData;
			}	// End sub

			/// <summary>
			/// Initializes plugin. Implements IToolRunner.Setup method
			/// </summary>
			/// <param name="mgrParams">Parameters for manager operation</param>
			/// <param name="taskParams">Parameters for the assigned task</param>
			/// <param name="statusTools">Tools for status reporting</param>
			public virtual void Setup(IMgrParams mgrParams, ITaskParams taskParams, IStatusFile statusTools)
			{
				m_MgrParams = mgrParams;
				m_TaskParams = taskParams;
				m_StatusTools = statusTools;
			}	// End sub

            protected bool UpdateMgrSettings()
            {
                bool bSuccess = true;

                if (m_MinutesBetweenConfigDBUpdates < 1)
                    m_MinutesBetweenConfigDBUpdates = 1;

                if (System.DateTime.Now.Subtract(m_LastConfigDBUpdate).TotalMinutes >= m_MinutesBetweenConfigDBUpdates)
                {
                    m_LastConfigDBUpdate = System.DateTime.Now;

                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Updating manager settings using Manager Control database");

                    if (!m_MgrParams.LoadMgrSettingsFromDB())
                    {
                        // Error retrieving settings from the manager control DB
                        string msg;
                        msg = "Error calling m_MgrSettings.LoadMgrSettingsFromDB to update manager settings";

                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);

                        bSuccess = false;
                    }
                    else
                    {
                        // Update the log level
                        int debugLevel = int.Parse(m_MgrParams.GetParam("debuglevel"));
                        clsLogTools.SetFileLogLevel(debugLevel);
                    }
                }

                return bSuccess;
            }                   

		#endregion
	}	// End class
}	// End namespace
