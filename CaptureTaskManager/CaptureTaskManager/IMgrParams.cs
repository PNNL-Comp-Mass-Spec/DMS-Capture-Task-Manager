
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 06/16/2009
//
// Last modified 06/16/2009
//*********************************************************************************************************

namespace CaptureTaskManager
{
	public interface IMgrParams
	{
		//*********************************************************************************************************
		// Defines interface for manager parameter handling
		//**********************************************************************************************************

		string GetParam(string ItemKey);
		void SetParam(string ItemKey, string ItemValue);

	}	// End interface
}	// End namespace
