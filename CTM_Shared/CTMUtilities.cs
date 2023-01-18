using System;
using System.IO;
using System.Runtime.CompilerServices;
using Pacifica.Core;
using PRISM.Logging;

// ReSharper disable UnusedMember.Global
namespace CaptureTaskManager
{
    // ReSharper disable once InconsistentNaming
    public static class CTMUtilities
    {
        /// <summary>
        /// When true, we are running on Linux and thus should not access any Windows features
        /// </summary>
        /// <remarks>Call EnableOfflineMode to set this to true</remarks>
        public static bool LinuxOS { get; private set; }

        /// <summary>
        /// When true, does not contact any databases or remote shares
        /// </summary>
        public static bool OfflineMode { get; private set; }

        private static string mAppDirectoryPath;

        /// <summary>
        /// Append additionalText to currentText
        /// </summary>
        /// <param name="currentText"></param>
        /// <param name="additionalText"></param>
        /// <param name="delimiter"></param>
        /// <returns>Combined text</returns>
        public static string AppendToString(string currentText, string additionalText, string delimiter = "; ")
        {
            if (string.IsNullOrEmpty(currentText))
            {
                return additionalText;
            }

            return currentText + delimiter + additionalText;
        }

        /// <summary>
        /// Convert a file size in bytes to gigabytes
        /// </summary>
        /// <param name="sizeBytes"></param>
        public static double BytesToGB(long sizeBytes)
        {
            return sizeBytes / 1024.0 / 1024 / 1024;
        }

        /// <summary>
        /// Decode a password
        /// </summary>
        /// <param name="encodedPwd">Encoded password</param>
        /// <returns>Clear text password</returns>
        public static string DecodePassword(string encodedPwd)
        {
            return Utilities.DecodePassword(encodedPwd);
        }

        /// <summary>
        /// Enable offline mode
        /// </summary>
        /// <remarks>When offline, does not contact any databases or remote shares</remarks>
        /// <param name="runningLinux">Set to True if running Linux</param>
        public static void EnableOfflineMode(bool runningLinux = true)
        {
            OfflineMode = true;
            LinuxOS = runningLinux;

            LogTools.OfflineMode = true;

            if (runningLinux)
            {
                Console.WriteLine("Offline mode enabled globally (running Linux)");
            }
            else
            {
                Console.WriteLine("Offline mode enabled globally");
            }
        }

        /// <summary>
        /// Returns the directory in which the entry assembly (typically the Program .exe file) resides
        /// </summary>
        /// <returns>Full directory path</returns>
        public static string GetAppDirectoryPath()
        {
            if (mAppDirectoryPath != null)
            {
                return mAppDirectoryPath;
            }

            mAppDirectoryPath = PRISM.AppUtils.GetAppDirectoryPath();

            return mAppDirectoryPath;
        }

        /// <summary>
        /// This function was added to debug remote share access issues
        /// The folder was accessible from some classes but not accessible from others
        /// </summary>
        /// <param name="pathToCheck"></param>
        /// <param name="callingFunction"></param>
        public static void VerifyFolder(string pathToCheck = @"\\Proto-2.emsl.pnl.gov\External_Orbitrap_Xfer\", [CallerMemberName] string callingFunction = "")
        {
            try
            {
                var directoryInfo = new DirectoryInfo(pathToCheck);
                string msg;

                if (directoryInfo.Exists)
                {
                    msg = "Directory exists [" + pathToCheck + "]; called from " + callingFunction;
                }
                else
                {
                    msg = "Directory not found [" + pathToCheck + "]; called from " + callingFunction;
                }

                LogTools.LogMessage(msg);
            }
            catch (Exception ex)
            {
                LogTools.LogError("Exception in VerifyFolder", ex);
            }
        }
    }
}
