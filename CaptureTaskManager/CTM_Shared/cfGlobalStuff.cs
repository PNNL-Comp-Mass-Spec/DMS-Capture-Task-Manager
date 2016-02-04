using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Text.RegularExpressions;

namespace CaptureTaskManager
{

    #region "Enums"

    //Status constants
    public enum EnumMgrStatus : short
    {
        Stopped,
        Stopped_Error,
        Running,
        Disabled_Local,
        Disabled_MC
    }

    public enum EnumTaskStatus : short
    {
        Stopped,
        Requesting,
        Running,
        Closing,
        Failed,
        No_Task
    }

    public enum EnumTaskStatusDetail : short
    {
        Retrieving_Resources,
        Running_Tool,
        Packaging_Results,
        Delivering_Results,
        No_Task
    }

    public enum EnumCloseOutType : short
    {
        CLOSEOUT_SUCCESS = 0,
        CLOSEOUT_FAILED = 1,
        CLOSEOUT_NOT_READY = 2,
        CLOSEOUT_NEED_TO_ABORT_PROCESSING = 3
    }

    public enum EnumEvalCode : short
    {
        EVAL_CODE_SUCCESS = 0,
        EVAL_CODE_FAILED = 1,
        EVAL_CODE_NOT_EVALUATED = 2,
        EVAL_CODE_NETWORK_ERROR_RETRY_CAPTURE = 3,
        EVAL_CODE_SUBMITTED_TO_MYEMSL = 4,
        EVAL_CODE_VERIFIED_IN_MYEMSL = 5,
        EVAL_CODE_SKIPPED_MYEMSL_UPLOAD = 6,
        EVAL_CODE_MYEMSL_IS_ALREADY_UP_TO_DATE = 7,
        EVAL_CODE_FAILURE_DO_NOT_RETRY = 8
    }

    public enum EnumRequestTaskResult : short
    {
        TaskFound = 0,
        NoTaskFound = 1,
        ResultError = 2
    }

    #endregion

    #region "Delegates"

    public delegate void StatusMonitorUpdateReceived(string msg);

    #endregion

    public class clsConversion
    {

        /// <summary>
        /// Convert string to bool; default false if an error
        /// </summary>
        /// <param name="Value"></param>
        /// <returns></returns>
        public static bool CBoolSafe(string Value)
        {
            return CBoolSafe(Value, false);
        }

        public static bool CBoolSafe(string Value, bool DefaultValue)
        {
            if (string.IsNullOrEmpty(Value))
                return DefaultValue;
            
            bool blnValue;
            if (bool.TryParse(Value, out blnValue))
                return blnValue;
            
            return DefaultValue;
        }

        public static int CIntSafe(string Value, int DefaultValue)
        {
            if (string.IsNullOrEmpty(Value))
                return DefaultValue;
            
            int intValue;
            if (int.TryParse(Value, out intValue))
                return intValue;
            
            return DefaultValue;
        }

        public static float CSngSafe(string Value, float DefaultValue)
        {
            var fValue = DefaultValue;

            if (string.IsNullOrEmpty(Value))
                return fValue;
            
            if (float.TryParse(Value, out fValue))
                return fValue;
            
            return fValue;
        }
        
        public static int GetDbValue(SqlDataReader reader, int fieldIndex, int valueIfNull)
        {
            if (Convert.IsDBNull(reader.GetValue(fieldIndex)))
                return valueIfNull;

            return (int)reader.GetValue(fieldIndex);
        }

        public static string GetDbValue(SqlDataReader reader, int fieldIndex, string valueIfNull)
        {
            if (Convert.IsDBNull(reader.GetValue(fieldIndex)))
                return valueIfNull;

            return (string)reader.GetValue(fieldIndex);
        }

        public static string PossiblyQuotePath(string strPath)
        {
            if (string.IsNullOrEmpty(strPath))
            {
                return string.Empty;

            }
            
            if (strPath.Contains(" "))
            {
                if (!strPath.StartsWith("\""))
                {
                    strPath = "\"" + strPath;
                }

                if (!strPath.EndsWith("\""))
                {
                    strPath += "\"";
                }
            }

            return strPath;
        }
        
    }

    public class clsErrors
    {

        /// <summary>
        /// Parses the .StackTrace text of the given exception to return a compact description of the current stack
        /// </summary>
        /// <param name="ex"></param>
        /// <param name="useMultiLine">True to show the stack track on multiple lines</param>
        /// <returns>String similar to "Stack trace: clsCodeTest.Test-:-clsCodeTest.TestException-:-clsCodeTest.InnerTestException in clsCodeTest.vb:line 86"</returns>
        /// <remarks></remarks>
        public static string GetExceptionStackTrace(Exception ex, bool useMultiLine = false)
        {
            if (useMultiLine)
                return PRISM.clsStackTraceFormatter.GetExceptionStackTraceMultiLine(ex);
            
            return PRISM.clsStackTraceFormatter.GetExceptionStackTrace(ex);
        }
    }

    public class clsUtilities
    {
        /// <summary>
        /// Returns the directory in which the entry assembly (typically the Program .exe file) resides 
        /// </summary>
        /// <returns>Full directory path</returns>
        public static string GetAppFolderPath()
        {
            var entryAssembly = System.Reflection.Assembly.GetEntryAssembly();

            var fiAssemblyFile = new FileInfo(entryAssembly.Location);

            if (string.IsNullOrEmpty(fiAssemblyFile.DirectoryName))
                return string.Empty;

            return fiAssemblyFile.DirectoryName;

        }

        public static void VerifyFolder(string callingFunction)
        {
            VerifyFolder(callingFunction, @"\\Proto-2.emsl.pnl.gov\External_Orbitrap_Xfer\");
        }

        public static void VerifyFolder(string callingFunction, string pathToCheck)
        {
            try
            {
                var diSourceFolder = new DirectoryInfo(pathToCheck);
                string msg;

                if (diSourceFolder.Exists)
                    msg = "Folder exists [" + pathToCheck + "]; called from " + callingFunction;
                else
                    msg = "Folder not found [" + pathToCheck + "]; called from " + callingFunction;

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);

                Console.WriteLine(msg);

            }
            catch (Exception ex)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Exception in VerifyFolder", ex);
            }
        }
    }
}