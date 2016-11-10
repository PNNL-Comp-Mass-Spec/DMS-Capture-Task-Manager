//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 06/18/2009
//
//*********************************************************************************************************

namespace CaptureTaskManager
{
    public interface IStatusFile
    {
        //*********************************************************************************************************
        // Interface used by classes that create and update task status file
        //**********************************************************************************************************

        #region "Events"

        event StatusMonitorUpdateReceived MonitorUpdateRequired;

        #endregion

        #region "Properties"

        string FileNamePath { get; set; }
        string MgrName { get; set; }
        EnumMgrStatus MgrStatus { get; set; }
        int CpuUtilization { get; set; }
        string Tool { get; set; }
        EnumTaskStatus TaskStatus { get; set; }
        float Duration { get; set; }
        float Progress { get; set; }
        string CurrentOperation { get; set; }
        EnumTaskStatusDetail TaskStatusDetail { get; set; }
        int JobNumber { get; set; }
        int JobStep { get; set; }
        string Dataset { get; set; }
        string MostRecentJobInfo { get; set; }
        int SpectrumCount { get; set; }
        bool LogToMsgQueue { get; set; }
        string FlagFilePath { get; }
        string MessageQueueURI { get; set; }
        string MessageQueueTopic { get; set; }

        #endregion

        #region "Methods"

        void ClearCachedInfo();
        void CreateStatusFlagFile();
        bool DeleteStatusFlagFile();
        bool DetectStatusFlagFile();
        void WriteStatusFile();
        void UpdateAndWrite(float PercentComplete);
        void UpdateAndWrite(EnumTaskStatusDetail Status, float PercentComplete);
        void UpdateStopped(bool MgrError);
        void UpdateDisabled(bool Local);
        void UpdateIdle();
        void InitStatusFromFile();

        #endregion
    }
}