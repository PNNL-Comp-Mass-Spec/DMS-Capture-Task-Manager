using System;
using CaptureTaskManager;
using System.Collections.Generic;

namespace ArchiveVerifyPlugin
{
	public class clsPluginMain : clsToolRunnerBase
	{
		//*********************************************************************************************************
		// Main class for plugin
		//**********************************************************************************************************

		#region "Constants and Enums"
	
		#endregion

		#region "Class-wide variables"
		clsToolReturnData mRetData = new clsToolReturnData();

		System.DateTime mLastStatusUpdate = System.DateTime.UtcNow;
		System.DateTime mQuameterStartTime = System.DateTime.UtcNow;

		#endregion

		#region "Constructors"
		public clsPluginMain()
			: base()
		{

		}

		#endregion

		#region "Methods"
		/// <summary>
		/// Runs the dataset info step tool
		/// </summary>
		/// <returns>Enum indicating success or failure</returns>
		public override clsToolReturnData RunTool()
		{
			string msg;
			
			msg = "Starting DatasetQualityPlugin.clsPluginMain.RunTool()";
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

			// Perform base class operations, if any
			mRetData = base.RunTool();
			if (mRetData.CloseoutType == EnumCloseOutType.CLOSEOUT_FAILED)
				return mRetData;
		
			if (m_DebugLevel >= 5)
			{
				msg = "Creating dataset info for dataset '" + m_Dataset + "'";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
			}

			// Perform work here
			bool success = false;

			if (success)
			{
				// Everything was good
				if (m_DebugLevel >= 4)
				{
					msg = "MyEMSL verification successful for dataset " + m_Dataset;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
				}
				mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
			}
			else
			{
				// There was a problem
				msg = "Problem verifying data in MyEMSL for dataset " + m_Dataset + ". See local log for details";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, msg);
				mRetData.EvalCode = EnumEvalCode.EVAL_CODE_FAILED;
				mRetData.EvalMsg = msg;
				mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
			}

			
			msg = "Completed clsPluginMain.RunTool()";
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

			return mRetData;

		}	// End sub

		#endregion

	}	// End class

}	// End namespace
