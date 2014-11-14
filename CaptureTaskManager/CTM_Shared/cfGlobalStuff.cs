using System;
using System.Collections.Generic;
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
            float fValue = DefaultValue;

            if (string.IsNullOrEmpty(Value))
                return fValue;
            
            if (float.TryParse(Value, out fValue))
                return fValue;
            
            return fValue;
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
        /// Parses the .StackTrace text of the given expression to return a compact description of the current stack
        /// </summary>
        /// <param name="objException"></param>
        /// <returns>String similar to "Stack trace: clsCodeTest.Test-:-clsCodeTest.TestException-:-clsCodeTest.InnerTestException in clsCodeTest.vb:line 86"</returns>
        /// <remarks></remarks>
        public static string GetExceptionStackTrace(Exception objException)
        {
            const string REGEX_FUNCTION_NAME = @"at ([^(]+)\(";
            const string REGEX_FILE_NAME = @"in .+\\(.+)";

            int intIndex;

            var lstFunctions = new List<string>();

            string strFinalFile = string.Empty;

            var reFunctionName = new Regex(REGEX_FUNCTION_NAME, RegexOptions.Compiled | RegexOptions.IgnoreCase);
            var reFileName = new Regex(REGEX_FILE_NAME, RegexOptions.Compiled | RegexOptions.IgnoreCase);

            // Process each line in objException.StackTrace
            // Populate lstFunctions with the function name of each line
            using (var trTextReader = new StringReader(objException.StackTrace))
            {
                while (trTextReader.Peek() > -1)
                {
                    string strLine = trTextReader.ReadLine();

                    if (!string.IsNullOrEmpty(strLine))
                    {
                        string strCurrentFunction = string.Empty;

                        Match objMatch = reFunctionName.Match(strLine);
                        if (objMatch.Success && objMatch.Groups.Count > 1)
                        {
                            strCurrentFunction = objMatch.Groups[1].Value;
                        }
                        else
                        {
                            // Look for the word " in "
                            intIndex = strLine.ToLower().IndexOf(" in ");
                            if (intIndex == 0)
                            {
                                // " in" not found; look for the first space after startIndex 4
                                intIndex = strLine.IndexOf(' ', 4);
                            }
                            if (intIndex == 0)
                            {
                                // Space not found; use the entire string
                                intIndex = strLine.Length - 1;
                            }

                            if (intIndex > 0)
                            {
                                strCurrentFunction = strLine.Substring(0, intIndex);
                            }

                        }

                        if (!String.IsNullOrEmpty(strCurrentFunction))
                        {
                            lstFunctions.Add(strCurrentFunction);
                        }

                        if (strFinalFile.Length == 0)
                        {
                            // Also extract the file name where the Exception occurred
                            objMatch = reFileName.Match(strLine);
                            if (objMatch.Success && objMatch.Groups.Count > 1)
                            {
                                strFinalFile = objMatch.Groups[1].Value;
                            }
                        }

                    }
                }
            }


            string strStackTrace = string.Empty;
            for (intIndex = lstFunctions.Count - 1; intIndex >= 0; intIndex -= 1)
            {
                if (strStackTrace.Length == 0)
                {
                    strStackTrace = "Stack trace: " + lstFunctions[intIndex];
                }
                else
                {
                    strStackTrace += "-:-" + lstFunctions[intIndex];
                }
            }

            if ((!String.IsNullOrEmpty(strStackTrace)) && !string.IsNullOrWhiteSpace(strFinalFile))
            {
                strStackTrace += " in " + strFinalFile;
            }

            return strStackTrace;

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

    }
}