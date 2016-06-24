
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 06/16/2009
//
//*********************************************************************************************************

namespace CaptureTaskManager
{
	public interface IMgrParams
	{
		//*********************************************************************************************************
		// Defines interface for manager parameter handling
		//**********************************************************************************************************

		#region "Properties"

		System.Collections.Generic.Dictionary<string, string> TaskDictionary { get; }
		
		#endregion

		#region "Methods"
        bool GetBooleanParam(string itemKey);
		string GetParam(string itemKey);
		string GetParam(string itemKey, string valueIfMissing);
        bool GetParam(string itemKey, bool valueIfMissing);
        int GetParam(string itemKey, int valueIfMissing);

		void SetParam(string itemKey, string itemValue);

        bool LoadMgrSettingsFromDB();
		bool LoadMgrSettingsFromDB(bool logConnectionErrors);

		#endregion

	}
}
