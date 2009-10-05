
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 06/18/2009
//
// Last modified 09/10/2009
//*********************************************************************************************************
using System;

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
		string FileNamePath { get;set; }
			string MgrName { get; set; }
			EnumMgrStatus MgrStatus { get; set; }
			int CpuUtilization { get; set; }
			string Tool { get; set; }
			EnumTaskStatus TaskStatus { get; set; }
			Single Duration { get; set; }
			Single Progress { get; set; }
			string CurrentOperation { get; set; }
			EnumTaskStatusDetail TaskStatusDetail { get; set; }
			int JobNumber { get; set; }
			int JobStep { get; set; }
			string Dataset { get; set; }
			string MostRecentJobInfo { get; set; }
			int SpectrumCount { get; set; }
			bool LogToMsgQueue { get; set; }
			string MessageQueueURI { get; set; }
			string MessageQueueTopic { get; set; }
		#endregion

		#region "Methods"
			void WriteStatusFile();
			void UpdateAndWrite(Single PercentComplete);
			void UpdateAndWrite(EnumTaskStatusDetail Status, Single PercentComplete);
			void UpdateAndWrite(EnumTaskStatusDetail Status, Single PercentComplete, int DTACount);
			void UpdateStopped(bool MgrError);
			void UpdateDisabled(bool Local);
			void UpdateIdle();
			void InitStatusFromFile();
		#endregion
	}	// End interface
}	// End namespace
