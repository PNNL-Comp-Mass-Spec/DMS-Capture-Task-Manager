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
		}

}	// End namespace