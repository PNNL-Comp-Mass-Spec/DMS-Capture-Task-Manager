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
			CLOSEOUT_NOT_READY = 2
		}

		public enum EnumEvalCode : short
		{
			EVAL_CODE_SUCCESS = 0,
			EVAL_CODE_FAILED = 1,
			EVAL_CODE_NOT_EVALUATED = 2
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
}	// End namespace