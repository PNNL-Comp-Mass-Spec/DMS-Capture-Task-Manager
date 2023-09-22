
// This file is used by Code Analysis to maintain SuppressMessage
// attributes that are applied to this project.
// Project-level suppressions either have no target or are given
// a specific target and scoped to a namespace, type, member, etc.

using System.Diagnostics.CodeAnalysis;

[assembly: SuppressMessage("Readability", "RCS1123:Add parentheses when necessary.", Justification = "Parentheses not required", Scope = "member", Target = "~M:CaptureToolPlugin.CaptureOps.OnFileCopyProgress(System.String,System.Single)")]
[assembly: SuppressMessage("Readability", "RCS1123:Add parentheses when necessary.", Justification = "Parentheses not required", Scope = "member", Target = "~M:CaptureToolPlugin.CaptureOps.PerformDSExistsActions(System.String,System.Boolean,System.Int32,System.Int32,System.Int32,CaptureTaskManager.ToolReturnData,System.Collections.Generic.IDictionary{System.IO.FileSystemInfo,System.String})~System.Boolean")]
[assembly: SuppressMessage("Roslynator", "RCS1123:Add parentheses when necessary.", Justification = "Parentheses not required", Scope = "member", Target = "~M:CaptureToolPlugin.DataCapture.CaptureBase.GetSleepInterval(System.Double,System.Int32)~System.Int32")]
[assembly: SuppressMessage("Roslynator", "RCS1179:Unnecessary assignment.", Justification = "Leave as-is for readability", Scope = "member", Target = "~M:CaptureToolPlugin.ShareConnection.ConnectToShare(System.String,System.String,System.String,CaptureTaskManager.EnumCloseOutType@,CaptureTaskManager.EnumEvalCode@)~System.Boolean")]
[assembly: SuppressMessage("Style", "IDE0066:Convert switch statement to expression", Justification = "Leave as-is for readability", Scope = "member", Target = "~M:CaptureToolPlugin.LCDataCapture.GetQuarter(System.DateTime)~System.Int32")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Acceptable name", Scope = "type", Target = "~T:CaptureToolPlugin.CaptureOps")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Acceptable name", Scope = "type", Target = "~T:CaptureToolPlugin.PluginMain")]
[assembly: SuppressMessage("Usage", "RCS1246:Use element access.", Justification = "Prefer to use .First()", Scope = "module")]
