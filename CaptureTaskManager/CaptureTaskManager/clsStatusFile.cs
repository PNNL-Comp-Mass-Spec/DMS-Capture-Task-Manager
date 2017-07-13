//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 09/10/2009
//
//*********************************************************************************************************

using System;
using System.Globalization;
using System.Xml;
using System.IO;

namespace CaptureTaskManager
{
    public class clsStatusFile : clsLoggerBase, IStatusFile
    {
        //*********************************************************************************************************
        // Class to handle status file updates
        //**********************************************************************************************************

        #region "Constants"

        public const string FLAG_FILE_NAME = "flagFile.txt";

        #endregion

        #region "Class variables"

        // Manager start time
        private DateTime m_MgrStartTime;

        #endregion

        #region "Properties"

        /// <summary>
        /// Status file name and location
        /// </summary>
        public string FileNamePath { get; set; }

        /// <summary>
        /// Manager name
        /// </summary>
        public string MgrName { get; set; }

        /// <summary>
        /// status value
        /// </summary>
        public EnumMgrStatus MgrStatus { get; set; } = EnumMgrStatus.Stopped;

        /// <summary>
        /// CPU Utilization
        /// </summary>
        public int CpuUtilization { get; set; }

        /// <summary>
        /// Step tool
        /// </summary>
        public string Tool { get; set; }

        /// <summary>
        /// Task status
        /// </summary>
        public EnumTaskStatus TaskStatus { get; set; } = EnumTaskStatus.No_Task;

        /// <summary>
        /// Task duration
        /// </summary>
        public float Duration { get; set; }

        /// <summary>
        /// Progess (value between 0 and 100)
        /// </summary>
        public float Progress { get; set; }

        /// <summary>
        /// Current operation description
        /// </summary>
        public string CurrentOperation { get; set; }

        /// <summary>
        /// Task status detail
        /// </summary>
        public EnumTaskStatusDetail TaskStatusDetail { get; set; } = EnumTaskStatusDetail.No_Task;

        /// <summary>
        /// Job number
        /// </summary>
        public int JobNumber { get; set; }

        /// <summary>
        /// Job step
        /// </summary>
        public int JobStep { get; set; }

        /// <summary>
        /// Dataset name
        /// </summary>
        public string Dataset { get; set; }

        /// <summary>
        /// Most recent job info
        /// </summary>
        public string MostRecentJobInfo { get; set; }

        /// <summary>
        /// Path to the flag file (it may or may not exist)
        /// </summary>
        public string FlagFilePath => Path.Combine(AppFolderPath(), FLAG_FILE_NAME);

        /// <summary>
        /// Message broker connection URI
        /// </summary>
        public string MessageQueueURI { get; set; }

        /// <summary>
        /// Broker topic for status reporting
        /// </summary>
        public string MessageQueueTopic { get; set; }

        /// <summary>
        /// If true, log to the message broker in addition to a file
        /// </summary>
        public bool LogToMsgQueue { get; set; }

        #endregion

        #region "Constructors"

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="fileLocation">Full path to status file</param>
        public clsStatusFile(string fileLocation)
        {
            FileNamePath = fileLocation;
            m_MgrStartTime = DateTime.Now;
            Progress = 0;
            Dataset = string.Empty;
            JobNumber = 0;
            Tool = string.Empty;
        }

        #endregion

        #region "Events"

        public event StatusMonitorUpdateReceived MonitorUpdateRequired;

        #endregion

        #region "Methods"

        /// <summary>
        /// Returns the folder path that contains the program .exe
        /// </summary>
        /// <returns></returns>
        private string AppFolderPath()
        {
            return Path.GetDirectoryName(System.Windows.Forms.Application.ExecutablePath);
        }

        /// <summary>
        /// Clears cached status info
        /// </summary>
        public void ClearCachedInfo()
        {
            Progress = 0;
            Dataset = "";
            JobNumber = 0;
            JobStep = 0;
            Tool = "";
            Duration = 0;

            // Only clear the recent job info if the variable is null
            if (MostRecentJobInfo == null)
            {
                MostRecentJobInfo = string.Empty;
            }
        }

        /// <summary>
        /// Converts the manager status enum to a string value
        /// </summary>
        /// <param name="statusEnum">An IStatusFile.EnumMgrStatus object</param>
        /// <returns>String representation of input object</returns>
        private string ConvertMgrStatusToString(EnumMgrStatus statusEnum)
        {
            return statusEnum.ToString("G");
        }

        /// <summary>
        /// Converts the task status enum to a string value
        /// </summary>
        /// <param name="statusEnum">An IStatusFile.EnumTaskStatus object</param>
        /// <returns>String representation of input object</returns>
        private string ConvertTaskStatusToString(EnumTaskStatus statusEnum)
        {
            return statusEnum.ToString("G");
        }

        /// <summary>
        /// Converts the manager status enum to a string value
        /// </summary>
        /// <param name="statusEnum">An IStatusFile.EnumTaskStatusDetail object</param>
        /// <returns>String representation of input object</returns>
        private string ConvertTaskDetailStatusToString(EnumTaskStatusDetail statusEnum)
        {
            return statusEnum.ToString("G");
        }

        /// <summary>
        /// Creates status flag file in same folder as .exe
        /// </summary>
        public void CreateStatusFlagFile()
        {
            var TestFileFi = new FileInfo(FlagFilePath);
            using (var sw = TestFileFi.AppendText())
            {
                sw.WriteLine(DateTime.Now.ToString(CultureInfo.InvariantCulture));
            }
        }

        /// <summary>
        /// Deletes the status flag file
        /// </summary>
        /// <returns></returns>
        public bool DeleteStatusFlagFile()
        {
            // Returns True if job request control flag file exists
            var strFlagFilePath = FlagFilePath;

            try
            {
                if (File.Exists(strFlagFilePath))
                {
                    File.Delete(strFlagFilePath);
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("DeleteStatusFlagFile, " + ex.Message);
                return false;
            }
        }

        public int GetProcessID()
        {
            var processID = System.Diagnostics.Process.GetCurrentProcess().Id;
            return processID;
        }

        /// <summary>
        /// Checks for presence of status flag file
        /// </summary>
        /// <returns>True if job request control flag file exists</returns>
        public bool DetectStatusFlagFile()
        {
            var strFlagFilePath = FlagFilePath;

            return File.Exists(strFlagFilePath);
        }

        /// <summary>
        /// Writes the status file
        /// </summary>
        public void WriteStatusFile()
        {
            // Note that we use this instead of using .ToString("o")
            // because .NET includes 7 digits of precision for the milliseconds,
            // and SQL Server only allows 3 digits of precision
            const string ISO_8601_DATE = "yyyy-MM-ddTHH:mm:ss.fffK";

            var xmlText = string.Empty;

            // Set up the XML writer
            try
            {
                // Create a memory stream to write the document in
                var memStream = new MemoryStream();
                using (var xWriter = new XmlTextWriter(memStream, System.Text.Encoding.UTF8))
                {
                    xWriter.Formatting = Formatting.Indented;
                    xWriter.Indentation = 2;

                    // Write the file
                    xWriter.WriteStartDocument(true);

                    // Root level element
                    xWriter.WriteStartElement("Root");
                    xWriter.WriteStartElement("Manager");
                    xWriter.WriteElementString("MgrName", MgrName);
                    xWriter.WriteElementString("MgrStatus", ConvertMgrStatusToString(MgrStatus));

                    // Write out times in the format 2017-07-06T23:23:14.337Z
                    xWriter.WriteElementString("LastUpdate", DateTime.UtcNow.ToString(ISO_8601_DATE));
                    xWriter.WriteElementString("LastStartTime", m_MgrStartTime.ToUniversalTime().ToString(ISO_8601_DATE));

                    xWriter.WriteElementString("CPUUtilization", CpuUtilization.ToString());
                    xWriter.WriteElementString("ProcessID", GetProcessID().ToString());
                    xWriter.WriteElementString("FreeMemoryMB", "0");
                    xWriter.WriteStartElement("RecentErrorMessages");
                    foreach (var ErrMsg in clsStatusData.ErrorQueue)
                    {
                        xWriter.WriteElementString("ErrMsg", ErrMsg);
                    }
                    xWriter.WriteEndElement();        // Error messages
                    xWriter.WriteEndElement();        // Manager section

                    xWriter.WriteStartElement("Task");
                    xWriter.WriteElementString("Tool", Tool);
                    xWriter.WriteElementString("Status", ConvertTaskStatusToString(TaskStatus));
                    xWriter.WriteElementString("Duration", Duration.ToString("##0.0"));
                    xWriter.WriteElementString("DurationMinutes", (60f * Duration).ToString("##0.0"));
                    xWriter.WriteElementString("Progress", Progress.ToString("##0.00"));
                    xWriter.WriteElementString("CurrentOperation", CurrentOperation);
                    xWriter.WriteStartElement("TaskDetails");
                    xWriter.WriteElementString("Status", ConvertTaskDetailStatusToString(TaskStatusDetail));
                    xWriter.WriteElementString("Job", JobNumber.ToString());
                    xWriter.WriteElementString("Step", JobStep.ToString());
                    xWriter.WriteElementString("Dataset", Dataset);
                    xWriter.WriteElementString("MostRecentLogMessage", clsStatusData.MostRecentLogMessage);
                    xWriter.WriteElementString("MostRecentJobInfo", MostRecentJobInfo);
                    xWriter.WriteEndElement();    // Task details section
                    xWriter.WriteEndElement();    // Task section
                    xWriter.WriteEndElement();    // Root section

                    // Close the document, but don't close the writer yet
                    xWriter.WriteEndDocument();
                    xWriter.Flush();

                    // Use a streamreader to copy the XML text to a string variable
                    memStream.Seek(0, SeekOrigin.Begin);
                    var MemStreamReader = new StreamReader(memStream);
                    xmlText = MemStreamReader.ReadToEnd();

                    MemStreamReader.Close();
                    memStream.Close();

                    //  Since xmlText now contains the XML, we can now safely close xWriter
                }

                PRISM.clsProgRunner.GarbageCollectNow();

                // Write the output file
                try
                {
                    using (var outFile = new StreamWriter(new FileStream(FileNamePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                    {
                        outFile.WriteLine(xmlText);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(@"Error writing status file: " + ex.Message);
                }
            }
            catch (Exception)
            {
                // Ignore errors here
            }

            // Log to a message queue
            if (LogToMsgQueue)
                LogStatusToMessageQueue(xmlText);
        }

        /// <summary>
        /// Writes the status to the message queue
        /// </summary>
        /// <param name="statusXML">A string contiaining the XML to write</param>
        protected void LogStatusToMessageQueue(string statusXML)
        {
            MonitorUpdateRequired?.Invoke(statusXML);
        }

        /// <summary>
        /// Updates status file
        /// (Overload to update when completion percentage is only change)
        /// </summary>
        /// <param name="percentComplete">Job completion percentage (between 0 and 100)</param>
        public void UpdateAndWrite(float percentComplete)
        {
            Progress = percentComplete;

            WriteStatusFile();
        }

        /// <summary>
        /// Updates status file
        /// (Overload to update file when status and completion percentage change)
        /// </summary>
        /// <param name="status">Job status enum</param>
        /// <param name="percentComplete">Job completion percentage (value between 0 and 100)</param>
        public void UpdateAndWrite(EnumTaskStatusDetail status, float percentComplete)
        {
            TaskStatusDetail = status;
            Progress = percentComplete;

            WriteStatusFile();
        }

        /// <summary>
        /// Sets status file to show mahager not running
        /// </summary>
        /// <param name="mgrError">TRUE if manager not running due to error; FALSE otherwise</param>
        public void UpdateStopped(bool mgrError)
        {
            ClearCachedInfo();

            if (mgrError)
            {
                MgrStatus = EnumMgrStatus.Stopped_Error;
            }
            else
            {
                MgrStatus = EnumMgrStatus.Stopped;
            }

            TaskStatus = EnumTaskStatus.No_Task;
            TaskStatusDetail = EnumTaskStatusDetail.No_Task;

            WriteStatusFile();
        }

        /// <summary>
        /// Updates status file to show manager disabled
        /// </summary>
        /// <param name="disabledLocally">TRUE if manager disabled locally, otherwise FALSE</param>
        public void UpdateDisabled(bool disabledLocally)
        {
            ClearCachedInfo();

            if (disabledLocally)
            {
                MgrStatus = EnumMgrStatus.Disabled_Local;
            }
            else
            {
                MgrStatus = EnumMgrStatus.Disabled_MC;
            }

            TaskStatus = EnumTaskStatus.No_Task;
            TaskStatusDetail = EnumTaskStatusDetail.No_Task;

            WriteStatusFile();
        }

        /// <summary>
        /// Updates status file to show manager in idle state
        /// </summary>
        public void UpdateIdle()
        {
            ClearCachedInfo();

            MgrStatus = EnumMgrStatus.Running;
            TaskStatus = EnumTaskStatus.No_Task;
            TaskStatusDetail = EnumTaskStatusDetail.No_Task;

            WriteStatusFile();
        }

        /// <summary>
        /// Initializes the status from a file, if file exists
        /// </summary>
        ///
        public void InitStatusFromFile()
        {
            // Verify status file exists
            if (!File.Exists(FileNamePath)) return;

            // Get data from status file
            try
            {
                // Read the input file
                var XmlStr = File.ReadAllText(FileNamePath);

                // Convert to an XML document
                var doc = new XmlDocument();
                doc.LoadXml(XmlStr);

                // Get the most recent log message
                var mostRecentMessageNode = doc.SelectSingleNode(@"//Task/TaskDetails/MostRecentLogMessage");
                if (mostRecentMessageNode != null)
                    clsStatusData.MostRecentLogMessage = mostRecentMessageNode.InnerText;

                // Get the most recent job info
                var mostRecentJobNode = doc.SelectSingleNode(@"//Task/TaskDetails/MostRecentJobInfo");
                if (mostRecentJobNode != null)
                    MostRecentJobInfo = mostRecentJobNode.InnerText;

                // Get the error messsages
                var recentErrMsgNode = doc.SelectNodes(@"//Manager/RecentErrorMessages/ErrMsg");
                if (recentErrMsgNode != null)
                    foreach (XmlNode Xn in recentErrMsgNode)
                    {
                        clsStatusData.AddErrorMessage(Xn.InnerText);
                    }
            }
            catch (Exception ex)
            {
                LogError("Exception reading status file", ex);
            }
        }

        #endregion
    }
}