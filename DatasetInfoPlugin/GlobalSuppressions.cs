﻿
// This file is used by Code Analysis to maintain SuppressMessage
// attributes that are applied to this project.
// Project-level suppressions either have no target or are given
// a specific target and scoped to a namespace, type, member, etc.

using System.Diagnostics.CodeAnalysis;

[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Acceptable name", Scope = "type", Target = "~T:DatasetInfoPlugin.PluginMain")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Acceptable name", Scope = "type", Target = "~T:DatasetInfoPlugin.DatasetInfoXmlMerger")]
[assembly: SuppressMessage("Usage", "RCS1246:Use element access.", Justification = "Prefer to use .First()", Scope = "module")]
[assembly: SuppressMessage("Readability", "RCS1123:Add parentheses when necessary.", Justification = "Parentheses not required", Scope = "member", Target = "~M:DatasetInfoPlugin.PluginMain.MsFileScanner_WarningEvent(System.String)")]
