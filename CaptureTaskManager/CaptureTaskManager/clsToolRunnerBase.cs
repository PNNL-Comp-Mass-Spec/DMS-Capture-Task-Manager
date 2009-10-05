
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
		#endregion
	}	// End class
}	// End namespace
