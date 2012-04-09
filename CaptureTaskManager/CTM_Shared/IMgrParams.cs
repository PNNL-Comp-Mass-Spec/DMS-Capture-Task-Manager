
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 06/16/2009
//
// Last modified 06/16/2009
//*********************************************************************************************************
using System.Collections.Generic;

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
		string GetParam(string ItemKey);
		string GetParam(string ItemKey, string valueIfMissing);
		void SetParam(string ItemKey, string ItemValue);

        bool LoadMgrSettingsFromDB();
		#endregion

	}	// End interface
}	// End namespace
