using System;
using System.Data;

namespace CaptureTaskManager
{
    public class clsCleanupMgrErrors : clsLoggerBase
    {
        #region "Constants"

        protected const string SP_NAME_REPORTMGRCLEANUP = "ReportManagerErrorCleanup";

        public enum eCleanupModeConstants
        {
            Disabled = 0,
            CleanupOnce = 1,
            CleanupAlways = 2
        }

        protected enum eCleanupActionCodeConstants
        {
            Start = 1,
            Success = 2,
            Fail = 3
        }

        #endregion

        #region "Class wide Variables"

        protected readonly bool mInitialized;
        protected string mMgrConfigDBConnectionString;

        protected readonly string mManagerName;
        protected readonly string mWorkingDirPath;

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
                LogMessage("Attempting to automatically clean the work directory");

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
                    // If successful, delete flagfile.txt 
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

        protected void ReportManagerErrorCleanup(eCleanupActionCodeConstants eMgrCleanupActionCode,
                                                 string strFailureMessage)
        {
            try
            {
                if (strFailureMessage == null)
                    strFailureMessage = string.Empty;

                var myConnection = new System.Data.SqlClient.SqlConnection(mMgrConfigDBConnectionString);
                myConnection.Open();

                //Set up the command object prior to SP execution
                var myCmd = new System.Data.SqlClient.SqlCommand
                {
                    CommandType = CommandType.StoredProcedure,
                    CommandText = SP_NAME_REPORTMGRCLEANUP,
                    Connection = myConnection
                };

                myCmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@Return", SqlDbType.Int)).Direction = ParameterDirection.ReturnValue;
                myCmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@ManagerName", SqlDbType.VarChar, 128)).Value = mManagerName;
                myCmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@State", SqlDbType.Int)).Value = eMgrCleanupActionCode;
                myCmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@FailureMsg", SqlDbType.VarChar, 512)).Value = strFailureMessage;
                myCmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@message", SqlDbType.VarChar, 512)).Direction = ParameterDirection.Output;

                //Execute the SP
                myCmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                if (mMgrConfigDBConnectionString == null)
                    mMgrConfigDBConnectionString = string.Empty;
                LogError("Exception calling " + SP_NAME_REPORTMGRCLEANUP +
                    " in ReportManagerErrorCleanup with connection string " + mMgrConfigDBConnectionString, ex);
            }
        }
    }
}