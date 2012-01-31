using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace FileProcessor
{
    // This class can be used to parse the text following the program name when a 
    //  program is started from the command line
    //
    // -------------------------------------------------------------------------------
    // Written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA)
    // Program started November 8, 2003

    // E-mail: matthew.monroe@pnl.gov or matt@alchemistmatt.com
    // Website: http://ncrr.pnl.gov/ or http://www.sysbio.org/resources/staff/
    // -------------------------------------------------------------------------------
    // 
    //=======================================================
    // Converted from VB.NET to C# using http://converter.telerik.com/
    // Service provided by Telerik (www.telerik.com)
    // Conversion powered by NRefactory.
    // Twitter: @telerik, @toddanglin
    // Facebook: facebook.com/telerik
    //=======================================================
    //
    // Last modified July 19, 2011

    public class clsParseCommandLine
    {

        public const char DEFAULT_SWITCH_CHAR = '/';

        public const char ALTERNATE_SWITCH_CHAR = '-';

        public const char DEFAULT_SWITCH_PARAM_CHAR = ':';
        protected System.Collections.Generic.Dictionary<string, string> mSwitches = new System.Collections.Generic.Dictionary<string, string>();

        protected System.Collections.Generic.List<string> mNonSwitchParameters = new System.Collections.Generic.List<string>();
        protected bool mShowHelp = false;

        protected bool mDebugMode = false;
        public bool NeedToShowHelp
        {
            get { return mShowHelp; }
        }

        public int ParameterCount
        {
            get { return mSwitches.Count; }
        }

        public int NonSwitchParameterCount
        {
            get { return mNonSwitchParameters.Count; }
        }

        public bool DebugMode
        {
            get { return mDebugMode; }
            set { mDebugMode = value; }
        }

        /// <summary>
        /// Compares the parameter names in objParameterList with the parameters at the command line
        /// </summary>
        /// <param name="objParameterList">Parameter list</param>
        /// <returns>True if any of the parameters are not present in strParameterList()</returns>
        public bool InvalidParametersPresent(System.Collections.Generic.List<string> objParameterList)
        {
            return InvalidParametersPresent(objParameterList.ToArray());
        }

        /// <summary>
        /// Compares the parameter names in strParameterList with the parameters at the command line
        /// </summary>
        /// <param name="strParameterList">Parameter list</param>
        /// <returns>True if any of the parameters are not present in strParameterList()</returns>
        public bool InvalidParametersPresent(string[] strParameterList)
        {
            bool blnCaseSensitive = false;
            return InvalidParametersPresent(strParameterList, blnCaseSensitive);
        }

        /// <summary>
        /// Compares the parameter names in strParameterList with the parameters at the command line
        /// </summary>
        /// <param name="strParameterList">Parameter list</param>
        /// <param name="blnCaseSensitive">True to perform case-sensitive matching of the parameter name</param>
        /// <returns>True if any of the parameters are not present in strParameterList()</returns>
        public bool InvalidParametersPresent(string[] strParameterList, bool blnCaseSensitive)
        {
            int intIndex = 0;
            bool blnMatchFound = false;

            try
            {
                System.Collections.Generic.Dictionary<string, string>.Enumerator iEnum = mSwitches.GetEnumerator();

                while (iEnum.MoveNext())
                {
                    blnMatchFound = false;
                    for (intIndex = 0; intIndex <= strParameterList.Length - 1; intIndex++)
                    {
                        if (blnCaseSensitive)
                        {
                            if (iEnum.Current.Key == strParameterList[intIndex])
                            {
                                blnMatchFound = true;
                                break;
                            }
                        }
                        else
                        {
                            if (iEnum.Current.Key.ToUpper() == strParameterList[intIndex].ToUpper())
                            {
                                blnMatchFound = true;
                                break;
                            }
                        }
                    }

                    if (!blnMatchFound)
                        return true;
                }

            }
            catch (System.Exception ex)
            {
                throw new System.Exception("Error in InvalidParametersPresent", ex);
            }

            return false;
        }

        /// <summary>
        /// Look for parameter on the command line
        /// </summary>
        /// <param name="strParameterName">Parameter name</param>
        /// <returns>True if present, otherwise false</returns>
        public bool IsParameterPresent(string strParameterName)
        {
            string strValue = string.Empty;
            bool blnCaseSensitive = false;
            return RetrieveValueForParameter(strParameterName, ref strValue, blnCaseSensitive);
        }

        /// <summary>
        /// Parse the parameters and switches at the command line; uses / for the switch character and : for the switch parameter character
        /// </summary>
        /// <returns>Returns True if any command line parameters were found; otherwise false</returns>
        /// <remarks>If /? or /help is found, then returns False and sets mShowHelp to True</remarks>
        public bool ParseCommandLine()
        {
            return ParseCommandLine(DEFAULT_SWITCH_CHAR, DEFAULT_SWITCH_PARAM_CHAR);
        }

        /// <summary>
        /// Parse the parameters and switches at the command line; uses : for the switch parameter character
        /// </summary>
        /// <returns>Returns True if any command line parameters were found; otherwise false</returns>
        /// <remarks>If /? or /help is found, then returns False and sets mShowHelp to True</remarks>
        public bool ParseCommandLine(char chSwitchStartChar)
        {
            return ParseCommandLine(chSwitchStartChar, DEFAULT_SWITCH_PARAM_CHAR);
        }

        /// <summary>
        /// Parse the parameters and switches at the command line
        /// </summary>
        /// <param name="chSwitchStartChar"></param>
        /// <param name="chSwitchParameterChar"></param>
        /// <returns>Returns True if any command line parameters were found; otherwise false</returns>
        /// <remarks>If /? or /help is found, then returns False and sets mShowHelp to True</remarks>
        public bool ParseCommandLine(char chSwitchStartChar, char chSwitchParameterChar)
        {
            string strCmdLine = string.Empty;
            string strKey = null;
            string strValue = null;

            int intCharLoc = 0;

            int intIndex = 0;
            string[] strParameters = null;

            bool blnSwitchParam = false;

            mSwitches.Clear();
            mNonSwitchParameters.Clear();

            try
            {
                try
                {
                    // .CommandLine() returns the full command line
                    strCmdLine = System.Environment.CommandLine;

                    // .GetCommandLineArgs splits the command line at spaces, though it keeps text between double quotes together
                    // Note that .NET will strip out the starting and ending double quote if the user provides a parameter like this:
                    // MyProgram.exe "C:\Program Files\FileToProcess"
                    //
                    // In this case, strParameters(1) will not have a double quote at the start but it will have a double quote at the end:
                    //  strParameters(1) = C:\Program Files\FileToProcess"

                    // One very odd feature of System.Environment.GetCommandLineArgs() is that if the command line looks like this:
                    //    MyProgram.exe "D:\My Folder\Subfolder\" /O:D:\OutputFolder
                    // Then strParameters will have:
                    //    strParameters(1) = D:\My Folder\Subfolder" /O:D:\OutputFolder
                    //
                    // To avoid this problem instead specify the command line as:
                    //    MyProgram.exe "D:\My Folder\Subfolder" /O:D:\OutputFolder
                    // which gives:
                    //    strParameters(1) = D:\My Folder\Subfolder
                    //    strParameters(2) = /O:D:\OutputFolder
                    //
                    // Due to the idiosyncrasies of .GetCommandLineArgs, we will instead use SplitCommandLineParams to do the splitting
                    // strParameters = System.Environment.GetCommandLineArgs()

                }
                catch (System.Exception ex)
                {
                    // In .NET 1.x, programs would fail if called from a network share
                    // This appears to be fixed in .NET 2.0 and above
                    // If an exception does occur here, we'll show the error message at the console, then sleep for 2 seconds

                    Console.WriteLine("------------------------------------------------------------------------------");
                    Console.WriteLine("This program cannot be run from a network share.  Please map a drive to the");
                    Console.WriteLine(" network share you are currently accessing or copy the program files and");
                    Console.WriteLine(" required DLL's to your local computer.");
                    Console.WriteLine(" Exception: " + ex.Message);
                    Console.WriteLine("------------------------------------------------------------------------------");

                    PauseAtConsole(5000, 1000);

                    mShowHelp = true;
                    return false;
                }

                if (mDebugMode)
                {
                    Console.WriteLine();
                    Console.WriteLine("Debugging command line parsing");
                    Console.WriteLine();
                }

                strParameters = SplitCommandLineParams(strCmdLine);

                if (mDebugMode)
                {
                    Console.WriteLine();
                }

                if (strCmdLine == null || strCmdLine.Length == 0)
                {
                    return false;
                }
                else if (strCmdLine.IndexOf(chSwitchStartChar + "?") > 0 | strCmdLine.ToLower().IndexOf(chSwitchStartChar + "help") > 0)
                {
                    mShowHelp = true;
                    return false;
                }

                // Parse the command line
                // Note that strParameters(0) is the path to the Executable for the calling program

                for (intIndex = 1; intIndex <= strParameters.Length - 1; intIndex++)
                {
                    if (strParameters[intIndex].Length > 0)
                    {
                        strKey = strParameters[intIndex].TrimStart(' ');
                        strValue = string.Empty;

                        if (strKey.StartsWith(chSwitchStartChar.ToString()))
                        {
                            blnSwitchParam = true;
                        }
                        else if (strKey.StartsWith(ALTERNATE_SWITCH_CHAR.ToString()) || strKey.StartsWith(DEFAULT_SWITCH_CHAR.ToString()))
                        {
                            blnSwitchParam = true;
                        }
                        else
                        {
                            // Parameter doesn't start with strSwitchStartChar or / or -
                            blnSwitchParam = false;
                        }

                        if (blnSwitchParam)
                        {
                            // Look for strSwitchParameterChar in strParameters[intIndex]
                            intCharLoc = strParameters[intIndex].IndexOf(chSwitchParameterChar);

                            if (intCharLoc >= 0)
                            {
                                // Parameter is of the form /I:MyParam or /I:"My Parameter" or -I:"My Parameter" or /MyParam:Setting
                                strValue = strKey.Substring(intCharLoc + 1).Trim();

                                // Remove any starting and ending quotation marks
                                strValue = strValue.Trim('"');

                                strKey = strKey.Substring(0, intCharLoc);
                            }
                            else
                            {
                                // Parameter is of the form /S or -S
                            }

                            // Remove the switch character from strKey
                            strKey = strKey.Substring(1).Trim();

                            if (mDebugMode)
                            {
                                Console.WriteLine("SwitchParam: " + strKey + "=" + strValue);
                            }

                            // Note: .Item() will add strKey if it doesn't exist (which is normally the case)
                            mSwitches[strKey] = strValue;
                        }
                        else
                        {
                            // Non-switch parameter since strSwitchParameterChar was not found and does not start with strSwitchStartChar

                            // Remove any starting and ending quotation marks
                            strKey = strKey.Trim('"');

                            if (mDebugMode)
                            {
                                Console.WriteLine("NonSwitchParam " + mNonSwitchParameters.Count + ": " + strKey);
                            }

                            mNonSwitchParameters.Add(strKey);
                        }

                    }
                }

            }
            catch (System.Exception ex)
            {
                throw new System.Exception("Error in ParseCommandLine", ex);
            }

            if (mDebugMode)
            {
                Console.WriteLine();
                Console.WriteLine("Switch Count = " + mSwitches.Count);
                Console.WriteLine("NonSwitch Count = " + mNonSwitchParameters.Count);
                Console.WriteLine();
            }

            if (mSwitches.Count + mNonSwitchParameters.Count > 0)
            {
                return true;
            }
            else
            {
                return false;
            }

        }


        public static void PauseAtConsole(int intMillisecondsToPause, int intMillisecondsBetweenDots)
        {
            int intIteration = 0;
            int intTotalIterations = 0;

            Console.WriteLine();
            Console.Write("Continuing in " + (intMillisecondsToPause / 1000.0).ToString("0") + " seconds ");

            try
            {
                if (intMillisecondsBetweenDots == 0)
                    intMillisecondsBetweenDots = intMillisecondsToPause;

                intTotalIterations = Convert.ToInt32(Math.Round((decimal)intMillisecondsToPause / intMillisecondsBetweenDots, 0));
            }
            catch
            {
                intTotalIterations = 1;
            }

            intIteration = 0;
            do
            {
                Console.Write('.');

                System.Threading.Thread.Sleep(intMillisecondsBetweenDots);

                intIteration += 1;
            } while (intIteration < intTotalIterations);

            Console.WriteLine();

        }

        /// <summary>
        /// Returns the value of the non-switch parameter at the given index
        /// </summary>
        /// <param name="intParameterIndex">Parameter index</param>
        /// <returns>The value of the parameter at the given index; empty string if no value or invalid index</returns>
        public string RetrieveNonSwitchParameter(int intParameterIndex)
        {
            string strValue = string.Empty;


            if (intParameterIndex < mNonSwitchParameters.Count)
            {
                strValue = mNonSwitchParameters[intParameterIndex];
            }

            if (strValue == null)
            {
                strValue = string.Empty;
            }

            return strValue;

        }

        /// <summary>
        /// Returns the parameter at the given index
        /// </summary>
        /// <param name="intParameterIndex">Parameter index</param>
        /// <param name="strKey">Parameter name (output)</param>
        /// <param name="strValue">Value associated with the parameter; empty string if no value (output)</param>
        /// <returns></returns>
        public bool RetrieveParameter(int intParameterIndex, ref string strKey, ref string strValue)
        {
            // Returns True if the parameter exists; returns false otherwise

            int intIndex = 0;

            try
            {
                strKey = string.Empty;
                strValue = string.Empty;

                if (intParameterIndex < mSwitches.Count)
                {
                    System.Collections.Generic.Dictionary<string, string>.Enumerator iEnum = mSwitches.GetEnumerator();

                    intIndex = 0;
                    while (iEnum.MoveNext())
                    {
                        if (intIndex == intParameterIndex)
                        {
                            strKey = iEnum.Current.Key;
                            strValue = iEnum.Current.Value;
                            return true;
                        }
                        intIndex += 1;
                    }
                }
                else
                {
                    return false;
                }
            }
            catch (System.Exception ex)
            {
                throw new System.Exception("Error in RetrieveParameter", ex);
            }

            return false;
        }

        /// <summary>
        /// Look for parameter on the command line and returns its value in strValue
        /// </summary>
        /// <param name="strKey">Parameter name</param>
        /// <param name="strValue">Value associated with the parameter; empty string if no value (output)</param>
        /// <returns>True if present, otherwise false</returns>
        public bool RetrieveValueForParameter(string strKey, ref string strValue)
        {
            return RetrieveValueForParameter(strKey, ref strValue, false);
        }

        /// <summary>
        /// Look for parameter on the command line and returns its value in strValue
        /// </summary>
        /// <param name="strKey">Parameter name</param>
        /// <param name="strValue">Value associated with the parameter; empty string if no value (output)</param>
        /// <param name="blnCaseSensitive">True to perform case-sensitive matching of the parameter name</param>
        /// <returns>True if present, otherwise false</returns>
        public bool RetrieveValueForParameter(string strKey, ref string strValue, bool blnCaseSensitive)
        {
            try
            {
                strValue = string.Empty;

                if (blnCaseSensitive)
                {
                    if (mSwitches.ContainsKey(strKey))
                    {
                        strValue = mSwitches[strKey];
                        return true;
                    }
                    else
                    {
                        return false;
                    }
                }
                else
                {
                    System.Collections.Generic.Dictionary<string, string>.Enumerator iEnum = mSwitches.GetEnumerator();

                    while (iEnum.MoveNext())
                    {
                        if (iEnum.Current.Key.ToUpper() == strKey.ToUpper())
                        {
                            strValue = iEnum.Current.Value;
                            return true;
                        }
                    }
                    return false;
                }
            }
            catch (System.Exception ex)
            {
                throw new System.Exception("Error in RetrieveValueForParameter", ex);
            }

        }

        protected string[] SplitCommandLineParams(string strCmdLine)
        {
            System.Collections.Generic.List<string> strParameters = new System.Collections.Generic.List<string>();
            string strParameter = null;

            int intIndexStart = 0;
            int intIndexEnd = 0;
            bool blnInsideDoubleQuotes = false;

            try
            {

                if (!string.IsNullOrEmpty(strCmdLine))
                {
                    blnInsideDoubleQuotes = false;

                    while (intIndexStart < strCmdLine.Length)
                    {
                        // Step through the characters to find the next space
                        // However, if we find a double quote, then stop checking for spaces

                        if (strCmdLine[intIndexEnd] == '"')
                        {
                            blnInsideDoubleQuotes = !blnInsideDoubleQuotes;
                        }

                        if (!blnInsideDoubleQuotes || intIndexEnd == strCmdLine.Length - 1)
                        {
                            if (strCmdLine[intIndexEnd] == ' ' || intIndexEnd == strCmdLine.Length - 1)
                            {
                                // Found the end of a parameter
                                strParameter = strCmdLine.Substring(intIndexStart, intIndexEnd - intIndexStart + 1).TrimEnd(' ');

                                if (strParameter.StartsWith('"'.ToString()))
                                {
                                    strParameter = strParameter.Substring(1);
                                }

                                if (strParameter.EndsWith('"'.ToString()))
                                {
                                    strParameter = strParameter.Substring(0, strParameter.Length - 1);
                                }

                                if (!string.IsNullOrEmpty(strParameter))
                                {
                                    if (mDebugMode)
                                    {
                                        Console.WriteLine("Param " + strParameters.Count + ": " + strParameter);
                                    }
                                    strParameters.Add(strParameter);
                                }

                                intIndexStart = intIndexEnd + 1;
                            }
                        }

                        intIndexEnd += 1;
                    }

                }

            }
            catch (System.Exception ex)
            {
                throw new System.Exception("Error in SplitCommandLineParams", ex);
            }

            return strParameters.ToArray();

        }
    }

}
