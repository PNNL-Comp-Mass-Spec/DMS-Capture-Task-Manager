
// This file is used by Code Analysis to maintain SuppressMessage
// attributes that are applied to this project.
// Project-level suppressions either have no target or are given
// a specific target and scoped to a namespace, type, member, etc.

using System.Diagnostics.CodeAnalysis;

[assembly: SuppressMessage("Design", "RCS1075:Avoid empty catch clause that catches System.Exception.", Justification = "It is safe to ignore a deletion failure here", Scope = "member", Target = "~M:CaptureTaskManager.ToolRunnerBase.StoreToolVersionInfoOneFileUseExe(System.String@,System.String,System.String)~System.Boolean")]
[assembly: SuppressMessage("Readability", "RCS1123:Add parentheses when necessary.", Justification = "Parentheses not required", Scope = "member", Target = "~M:CaptureTaskManager.RunDosProgram.RunProgram(System.String,System.String,System.String,System.Boolean,System.Int32)~System.Boolean")]
[assembly: SuppressMessage("Readability", "RCS1123:Add parentheses when necessary.", Justification = "Parentheses not required", Scope = "member", Target = "~M:CaptureTaskManager.ToolRunnerBase.FileTools_StatusEvent(System.String)")]
[assembly: SuppressMessage("Style", "IDE0066:Convert switch statement to expression", Justification = "Leave as-is for readability", Scope = "member", Target = "~M:CaptureTaskManager.DatasetFileSearchTool.FindDatasetFileOrDirectory(System.String,System.String,CaptureTaskManager.InstrumentClass)~CaptureTaskManager.DatasetInfo")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Acceptable name", Scope = "type", Target = "~T:CaptureTaskManager.Conversion")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Acceptable name", Scope = "type", Target = "~T:CaptureTaskManager.CTMUtilities")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Acceptable name", Scope = "type", Target = "~T:CaptureTaskManager.Errors")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Acceptable name", Scope = "type", Target = "~T:CaptureTaskManager.FailedResultsCopier")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Acceptable name", Scope = "type", Target = "~T:CaptureTaskManager.InstrumentClassInfo")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Acceptable name", Scope = "type", Target = "~T:CaptureTaskManager.LoggerBase")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Acceptable name", Scope = "type", Target = "~T:CaptureTaskManager.RunDosProgram")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Acceptable name", Scope = "type", Target = "~T:CaptureTaskManager.StatusData")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Acceptable name", Scope = "type", Target = "~T:CaptureTaskManager.ToolReturnData")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Acceptable name", Scope = "type", Target = "~T:CaptureTaskManager.ToolRunnerBase")]
