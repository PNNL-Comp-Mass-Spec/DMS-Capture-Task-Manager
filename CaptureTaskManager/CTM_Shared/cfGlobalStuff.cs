using System;
using System.Collections.Generic;

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
			EVAL_CODE_NETWORK_ERROR_RETRY_CAPTURE = 3
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
				bool blnValue = DefaultValue;

				if (string.IsNullOrEmpty(Value))
					return DefaultValue;
				else
				{
					if (bool.TryParse(Value, out blnValue))
						return blnValue;
					else
						return DefaultValue;
				}
			}

			public static int CIntSafe(string Value, int DefaultValue)
			{
				int intValue = DefaultValue;

				if (string.IsNullOrEmpty(Value))
					return DefaultValue;
				else
				{
					if (int.TryParse(Value, out intValue))
						return intValue;
					else
						return DefaultValue;
				}
			}

			public static float CSngSafe(string Value, float DefaultValue)
			{
				float fValue = DefaultValue;

				if (string.IsNullOrEmpty(Value))
					return fValue;
				else
				{
					if (float.TryParse(Value, out fValue))
						return fValue;
					else
						return fValue;
				}
			}

			public static string PossiblyQuotePath(string strPath)
			{
				if (string.IsNullOrEmpty(strPath))
				{
					return string.Empty;

				}
				else
				{
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

		}

		public class clsErrors
		{

			/// <summary>
			/// Parses the .StackTrace text of the given expression to return a compact description of the current stack
			/// </summary>
			/// <param name="objException"></param>
			/// <returns>String similar to "Stack trace: clsCodeTest.Test-:-clsCodeTest.TestException-:-clsCodeTest.InnerTestException in clsCodeTest.vb:line 86"</returns>
			/// <remarks></remarks>
			public static string GetExceptionStackTrace(System.Exception objException)
			{
				const string REGEX_FUNCTION_NAME = @"at ([^(]+)\(";
				const string REGEX_FILE_NAME = @"in .+\\(.+)";

				int intIndex = 0;

				List<string> lstFunctions = new List<string>();

				string strCurrentFunction = string.Empty;
				string strFinalFile = string.Empty;

				string strLine = string.Empty;
				string strStackTrace = string.Empty;

				System.Text.RegularExpressions.Regex reFunctionName = new System.Text.RegularExpressions.Regex(REGEX_FUNCTION_NAME, System.Text.RegularExpressions.RegexOptions.Compiled | System.Text.RegularExpressions.RegexOptions.IgnoreCase);
				System.Text.RegularExpressions.Regex reFileName = new System.Text.RegularExpressions.Regex(REGEX_FILE_NAME, System.Text.RegularExpressions.RegexOptions.Compiled | System.Text.RegularExpressions.RegexOptions.IgnoreCase);
				System.Text.RegularExpressions.Match objMatch;

				// Process each line in objException.StackTrace
				// Populate lstFunctions with the function name of each line
				using (System.IO.StringReader trTextReader = new System.IO.StringReader(objException.StackTrace))
				{
					while (trTextReader.Peek() > -1)
					{
						strLine = trTextReader.ReadLine();

						if (!string.IsNullOrEmpty(strLine))
						{
							strCurrentFunction = string.Empty;

							objMatch = reFunctionName.Match(strLine);
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
									intIndex = strLine.IndexOf(" ", 4);
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

				
				strStackTrace = string.Empty;
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

}	// End namespace