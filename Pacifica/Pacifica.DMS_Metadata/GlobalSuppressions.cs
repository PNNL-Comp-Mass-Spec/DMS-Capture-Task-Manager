// This file is used by Code Analysis to maintain SuppressMessage
// attributes that are applied to this project.
// Project-level suppressions either have no target or are given
// a specific target and scoped to a namespace, type, member, etc.

using System.Diagnostics.CodeAnalysis;

[assembly: SuppressMessage("Usage", "RCS1246:Use element access.", Justification = "Prefer to use .First()", Scope = "module")]
[assembly: SuppressMessage("Design", "RCS1075:Avoid empty catch clause that catches System.Exception.", Justification = "Safe to ignore lock file deletion issues", Scope = "member", Target = "~M:Pacifica.DMS_Metadata.DMSMetadataObject.DeleteLockFiles")]
[assembly: SuppressMessage("Readability", "RCS1123:Add parentheses when necessary.", Justification = "Parentheses not required", Scope = "member", Target = "~M:Pacifica.DMS_Metadata.MyEMSLUploader._mdContainer_ProgressEvent(System.String,System.Single)")]
[assembly: SuppressMessage("Readability", "RCS1123:Add parentheses when necessary.", Justification = "Parentheses not required", Scope = "member", Target = "~M:Pacifica.DMS_Metadata.MyEMSLUploader.myEMSLUpload_StatusUpdate(System.Object,Pacifica.Core.StatusEventArgs)")]
