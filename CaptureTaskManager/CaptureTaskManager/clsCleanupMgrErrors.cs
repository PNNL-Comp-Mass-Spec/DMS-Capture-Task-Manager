using System;
using System.Data;

namespace CaptureTaskManager
{
	public class clsCleanupMgrErrors
	{

		#region "Constants"

		protected const string SP_NAME_REPORTMGRCLEANUP = "ReportManagerErrorCleanup";
		public enum eCleanupModeConstants
		{
			Disabled = 0,
			CleanupOnce = 1,
			CleanupAlways = 2
		}

		public enum eCleanupActionCodeConstants
		{
			Start = 1,
			Success = 2,
			Fail = 3
		}
		#endregion

		#region "Class wide Variables"

		protected bool mInitialized;
		protected string mMgrConfigDBConnectionString;

		protected string mManagerName;
		protected string mMgrFolderPath = string.Empty;

		protected string mWorkingDirPath;


		private readonly IStatusFile m_StatusFile;
		#endregion


		public clsCleanupMgrErrors(string strMgrConfigDBConnectionString,
								   string strManagerName,
								   string strWorkingDirPath,
								   IStatusFile oStatusFile)
		{
			if (string.IsNullOrEmpty(strMgrConfigDBConnectionString))
				throw new Exception("Manager config DB connection string is not defined");

			if (string.IsNullOrEmpty(strManagerName))
				throw new Exception("Manager name is not defined");

			mMgrConfigDBConnectionString = string.Copy(strMgrConfigDBConnectionString);
			mManagerName = string.Copy(strManagerName);

			mWorkingDirPath = strWorkingDirPath;

			m_StatusFile = oStatusFile;

			mInitialized = true;

		}

		public bool AutoCleanupManagerErrors(int ManagerErrorCleanupMode)
		{
			eCleanupModeConstants eManagerErrorCleanupMode;

			switch (ManagerErrorCleanupMode)
			{
				case 0:
					eManagerErrorCleanupMode = eCleanupModeConstants.Disabled;
					break;
				case 1:
					eManagerErrorCleanupMode = eCleanupModeConstants.CleanupOnce;
					break;
				case 2:
					eManagerErrorCleanupMode = eCleanupModeConstants.CleanupAlways;
					break;
				default:
					eManagerErrorCleanupMode = eCleanupModeConstants.Disabled;
					break;
			}

			return AutoCleanupManagerErrors(eManagerErrorCleanupMode);


		}

		public bool AutoCleanupManagerErrors(eCleanupModeConstants eManagerErrorCleanupMode)
		{
			var blnSuccess = false;

		    if (!mInitialized)
				return false;


			if (eManagerErrorCleanupMode != eCleanupModeConstants.Disabled)
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Attempting to automatically clean the work directory");

				// Call SP ReportManagerErrorCleanup @ActionCode=1
				ReportManagerErrorCleanup(eCleanupActionCodeConstants.Start);

				// Delete all folders and subfolders in work folder
			    string strFailureMessage;
			    blnSuccess = clsToolRunnerBase.CleanWorkDir(mWorkingDirPath, 1, out strFailureMessage);

				if (!blnSuccess)
				{
					if (string.IsNullOrEmpty(strFailureMessage))
						strFailureMessage = "unable to clear work directory";
				}
				else
				{
					// If successful, then delete flagfile.txt 

					blnSuccess = m_StatusFile.DeleteStatusFlagFile();
					if (!blnSuccess)
					{
						strFailureMessage = "error deleting " + clsStatusFile.FLAG_FILE_NAME;
					}
				}


				// If successful, then call SP with ReportManagerErrorCleanup @ActionCode=2 
				//    otherwise call SP ReportManagerErrorCleanup with @ActionCode=3

				if (blnSuccess)
				{
					ReportManagerErrorCleanup(eCleanupActionCodeConstants.Success);
				}
				else
				{
					ReportManagerErrorCleanup(eCleanupActionCodeConstants.Fail, strFailureMessage);
				}

			}

			return blnSuccess;

		}

		protected void ReportManagerErrorCleanup(eCleanupActionCodeConstants eMgrCleanupActionCode)
		{
			ReportManagerErrorCleanup(eMgrCleanupActionCode, string.Empty);
		}


		protected void ReportManagerErrorCleanup(eCleanupActionCodeConstants eMgrCleanupActionCode, string strFailureMessage)
		{

		    try
			{
				if (strFailureMessage == null)
					strFailureMessage = string.Empty;

				var MyConnection = new System.Data.SqlClient.SqlConnection(mMgrConfigDBConnectionString);
				MyConnection.Open();

				//Set up the command object prior to SP execution
                var MyCmd = new System.Data.SqlClient.SqlCommand();
				{
					MyCmd.CommandType = CommandType.StoredProcedure;
					MyCmd.CommandText = SP_NAME_REPORTMGRCLEANUP;
					MyCmd.Connection = MyConnection;

					MyCmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@Return", SqlDbType.Int));
					MyCmd.Parameters["@Return"].Direction = ParameterDirection.ReturnValue;

					MyCmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@ManagerName", SqlDbType.VarChar, 128));
					MyCmd.Parameters["@ManagerName"].Direction = ParameterDirection.Input;
					MyCmd.Parameters["@ManagerName"].Value = mManagerName;

					MyCmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@State", SqlDbType.Int));
					MyCmd.Parameters["@State"].Direction = ParameterDirection.Input;
					MyCmd.Parameters["@State"].Value = eMgrCleanupActionCode;

					MyCmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@FailureMsg", SqlDbType.VarChar, 512));
					MyCmd.Parameters["@FailureMsg"].Direction = ParameterDirection.Input;
					MyCmd.Parameters["@FailureMsg"].Value = strFailureMessage;

					MyCmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@message", SqlDbType.VarChar, 512));
					MyCmd.Parameters["@message"].Direction = ParameterDirection.Output;
					MyCmd.Parameters["@message"].Value = string.Empty;
				}

				//Execute the SP
				MyCmd.ExecuteNonQuery();

			}
			catch (Exception ex)
			{
				if (mMgrConfigDBConnectionString == null)
					mMgrConfigDBConnectionString = string.Empty;
				var strErrorMessage = "Exception calling " + SP_NAME_REPORTMGRCLEANUP + " in ReportManagerErrorCleanup with connection string " + mMgrConfigDBConnectionString;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strErrorMessage + ex.Message);
			}

		}

	}
}
