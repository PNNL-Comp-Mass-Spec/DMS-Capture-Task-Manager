﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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

            protected bool mInitialized = false;
            protected string mMgrConfigDBConnectionString = string.Empty;

            protected string mManagerName = string.Empty;
            protected string mMgrFolderPath = string.Empty;

            protected string mWorkingDirPath = string.Empty;


            private IStatusFile m_StatusFile;
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
                    eManagerErrorCleanupMode = clsCleanupMgrErrors.eCleanupModeConstants.Disabled;
                    break;
                case 1:
                    eManagerErrorCleanupMode = clsCleanupMgrErrors.eCleanupModeConstants.CleanupOnce;
                    break;
                case 2:
                    eManagerErrorCleanupMode = clsCleanupMgrErrors.eCleanupModeConstants.CleanupAlways;
                    break;
                default:
                    eManagerErrorCleanupMode = clsCleanupMgrErrors.eCleanupModeConstants.Disabled;
                    break;
            }

            return AutoCleanupManagerErrors(eManagerErrorCleanupMode);


        }

        public bool AutoCleanupManagerErrors(eCleanupModeConstants eManagerErrorCleanupMode)
        {
            bool blnSuccess = false;
            string strFailureMessage = string.Empty;

            if (!mInitialized)
                return false;


            if (eManagerErrorCleanupMode != eCleanupModeConstants.Disabled)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Attempting to automatically clean the work directory");

                // Call SP ReportManagerErrorCleanup @ActionCode=1
                ReportManagerErrorCleanup(eCleanupActionCodeConstants.Start);

                // Delete all folders and subfolders in work folder
                blnSuccess = this.CleanWorkDir(mWorkingDirPath, 1, ref strFailureMessage);

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

        public bool CleanWorkDir(string WorkDir, float HoldoffSeconds, ref string strFailureMessage)
        {

	        System.IO.DirectoryInfo diWorkFolder = default(System.IO.DirectoryInfo);
	        int HoldoffMilliseconds = 0;

	        string strCurrentFile = string.Empty;
	        string strCurrentSubfolder = string.Empty;

	        strFailureMessage = string.Empty;

	        try {
		        HoldoffMilliseconds = Convert.ToInt32(HoldoffSeconds * 1000);
		        if (HoldoffMilliseconds < 100)
			        HoldoffMilliseconds = 100;
		        if (HoldoffMilliseconds > 300000)
			        HoldoffMilliseconds = 300000;
	        } catch (Exception) {
		        HoldoffMilliseconds = 10000;
	        }

	        //Try to ensure there are no open objects with file handles
	        GC.Collect();
	        GC.WaitForPendingFinalizers();
	        System.Threading.Thread.Sleep(HoldoffMilliseconds);

	        diWorkFolder = new System.IO.DirectoryInfo(WorkDir);

	        //Delete the files
	        try {
		        foreach (System.IO.FileInfo fiFile in diWorkFolder.GetFiles()) {
			        try {
				        fiFile.Delete();
			        } catch (Exception) {
				        // Make sure the readonly bit is not set
				        // The manager will try to delete the file the next time is starts
                        fiFile.Attributes = fiFile.Attributes & (~System.IO.FileAttributes.ReadOnly);
			        }
		        }
	        } catch (Exception Ex) {
		        strFailureMessage = "Error deleting files in working directory";
		        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsGlobal.ClearWorkDir(), " + strFailureMessage + " " + WorkDir + ": " + Ex.Message);
		        return false;
	        }

	        //Delete the sub directories
	        try {
		        foreach (System.IO.DirectoryInfo diSubDirectory in diWorkFolder.GetDirectories()) {
			        diSubDirectory.Delete(true);
		        }
	        } catch (Exception Ex) {
		        strFailureMessage = "Error deleting subfolder " + strCurrentSubfolder;
		        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strFailureMessage + " in working directory: " + Ex.Message);
		        return false;
	        }

	        return true;

        }

        protected void ReportManagerErrorCleanup(eCleanupActionCodeConstants eMgrCleanupActionCode)
        {
            ReportManagerErrorCleanup(eMgrCleanupActionCode, string.Empty);
        }


        protected void ReportManagerErrorCleanup(eCleanupActionCodeConstants eMgrCleanupActionCode, string strFailureMessage)
        {
            System.Data.SqlClient.SqlConnection MyConnection = default(System.Data.SqlClient.SqlConnection);
            System.Data.SqlClient.SqlCommand MyCmd = new System.Data.SqlClient.SqlCommand();
            int RetVal = 0;

            try
            {
                if (strFailureMessage == null)
                    strFailureMessage = string.Empty;

                MyConnection = new System.Data.SqlClient.SqlConnection(mMgrConfigDBConnectionString);
                MyConnection.Open();

                //Set up the command object prior to SP execution
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
                RetVal = MyCmd.ExecuteNonQuery();

            }
            catch (System.Exception ex)
            {
                if (mMgrConfigDBConnectionString == null)
                    mMgrConfigDBConnectionString = string.Empty;
                string strErrorMessage = "Exception calling " + SP_NAME_REPORTMGRCLEANUP + " in ReportManagerErrorCleanup with connection string " + mMgrConfigDBConnectionString;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strErrorMessage + ex.Message);
            }

        }

    }
}