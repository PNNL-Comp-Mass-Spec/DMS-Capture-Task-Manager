
// This file is used by Code Analysis to maintain SuppressMessage
// attributes that are applied to this project.
// Project-level suppressions either have no target or are given
// a specific target and scoped to a namespace, type, member, etc.

using System.Diagnostics.CodeAnalysis;

[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Acceptable name", Scope = "type", Target = "~T:CaptureTaskManager.clsConversion")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Acceptable name", Scope = "type", Target = "~T:CaptureTaskManager.clsErrors")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Acceptable name", Scope = "type", Target = "~T:CaptureTaskManager.clsUtilities")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Acceptable name", Scope = "type", Target = "~T:CaptureTaskManager.clsInstrumentClassInfo")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Acceptable name", Scope = "type", Target = "~T:CaptureTaskManager.clsFailedResultsCopier")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Acceptable name", Scope = "type", Target = "~T:CaptureTaskManager.clsLoggerBase")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Acceptable name", Scope = "type", Target = "~T:CaptureTaskManager.clsRunDosProgram")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Acceptable name", Scope = "type", Target = "~T:CaptureTaskManager.clsToolRunnerBase")]
[assembly: SuppressMessage("Design", "RCS1075:Avoid empty catch clause that catches System.Exception.", Justification = "It is safe to ignore a deletion failure here", Scope = "member", Target = "~M:CaptureTaskManager.clsToolRunnerBase.StoreToolVersionInfoOneFileUseExe(System.String@,System.String,System.String)~System.Boolean")]
[assembly: SuppressMessage("Readability", "RCS1123:Add parentheses when necessary.", Justification = "Parentheses not required", Scope = "member", Target = "~M:CaptureTaskManager.clsRunDosProgram.RunProgram(System.String,System.String,System.String,System.Boolean,System.Int32)~System.Boolean")]
[assembly: SuppressMessage("Readability", "RCS1123:Add parentheses when necessary.", Justification = "Parentheses not required", Scope = "member", Target = "~M:CaptureTaskManager.clsToolRunnerBase.FileTools_StatusEvent(System.String)")]
[assembly: SuppressMessage("Style", "IDE0059:Unnecessary assignment of a value", Justification = "This is not unnecessary", Scope = "member", Target = "~M:CaptureTaskManager.clsUtilities.GetDataTableByCmd(System.Data.SqlClient.SqlCommand,System.String,System.Int16,System.Data.DataTable@,System.Int32,System.String)~System.Boolean")]
